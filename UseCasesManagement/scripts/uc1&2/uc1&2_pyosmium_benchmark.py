import osmium as o
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from pathlib import Path
import sys
import osmnx as ox

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
RAW_DATA_DIR = WORKING_ROOT / 'data' / 'raw'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

# Handler for the first pass to collect required IDs
class IdCollectorHandler(o.SimpleHandler):
    def __init__(self):
        super(IdCollectorHandler, self).__init__()
        self.required_nodes = set()
        self.required_ways = set()

    def relation(self, r):
        if r.tags.get('type') == 'multipolygon' and 'building' in r.tags:
            for member in r.members:
                if member.type == 'w':
                    self.required_ways.add(member.ref)

    def way(self, w):
        if 'building' in w.tags:
            self.required_ways.add(w.id)

    def collect_way_nodes(self, pbf_filepath):
        class NodeCollector(o.SimpleHandler):
            def __init__(self, required_ways_set):
                super(NodeCollector, self).__init__()
                self.required_ways = required_ways_set
                self.required_nodes = set()

            def way(self, w):
                if w.id in self.required_ways:
                    for node in w.nodes:
                        self.required_nodes.add(node.ref)

        node_collector = NodeCollector(self.required_ways)
        node_collector.apply_file(str(pbf_filepath))
        self.required_nodes = node_collector.required_nodes

# Handler for the second pass to build geometries efficiently
class BuildingGeometryHandler(o.SimpleHandler):
    def __init__(self, required_nodes, required_ways):
        super(BuildingGeometryHandler, self).__init__()
        self.required_nodes = required_nodes
        self.required_ways = required_ways
        self.nodes_cache = {}
        self.ways_cache = {}
        self.relations_cache = {}  # Cache relations for final assembly
        self.used_ways_in_relations = set()
        self.buildings = []

    def node(self, n):
        if n.id in self.required_nodes:
            self.nodes_cache[n.id] = (n.location.lon, n.location.lat)

    def way(self, w):
        if w.id in self.required_ways:
            self.ways_cache[w.id] = {'nodes': [n.ref for n in w.nodes], 'tags': dict(w.tags)}

    def relation(self, r):
        if r.tags.get('type') == 'multipolygon' and 'building' in r.tags:
            self.relations_cache[r.id] = {'members': [(m.ref, m.type, m.role) for m in r.members], 'tags': dict(r.tags)}

    def get_geodataframe(self):
        for rel_data in self.relations_cache.values():
            try:
                outer_rings, inner_rings = [], []
                for way_id, way_type, way_role in rel_data['members']:
                    if way_type == 'w' and way_id in self.ways_cache:
                        self.used_ways_in_relations.add(way_id)
                        way = self.ways_cache[way_id]
                        points = [self.nodes_cache[node_id] for node_id in way['nodes'] if node_id in self.nodes_cache]
                        if len(points) >= 4:
                            if way_role == 'outer':
                                outer_rings.append(points)
                            else:
                                inner_rings.append(points)
                if outer_rings:
                    polygon = Polygon(outer_rings[0], inner_rings) if len(outer_rings) == 1 else MultiPolygon(
                        [Polygon(o) for o in outer_rings])
                    self.buildings.append({'geometry': polygon, 'tags': rel_data['tags']})
            except Exception:
                continue

        for way_id, way_data in self.ways_cache.items():
            if 'building' in way_data['tags'] and way_id not in self.used_ways_in_relations:
                try:
                    points = [self.nodes_cache[node_id] for node_id in way_data['nodes'] if node_id in self.nodes_cache]
                    if len(points) >= 4:
                        self.buildings.append({'geometry': Polygon(points), 'tags': way_data['tags']})
                except Exception:
                    continue

        return gpd.GeoDataFrame(self.buildings, crs="EPSG:4326")

def run_pyosmium_ingestion_and_filtering(place_name, pbf_filepath, num_runs=100):
    """
    Runs the data ingestion and filtering benchmark using the memory-efficient two-pass method.
    """
    place_name_clean = place_name.split(',')[0]
    print(f"Testing PyOsmium + GeoPandas ingestion & filtering for {place_name_clean} buildings over {num_runs} runs.")

    if not pbf_filepath.exists():
        print(f"ERROR: PBF file not found at {pbf_filepath}. Aborting.")
        return

    # Perform the first pass once before the benchmark starts
    print("\nStarting first pass to collect required IDs.")
    id_handler = IdCollectorHandler()
    id_handler.apply_file(str(pbf_filepath))
    id_handler.collect_way_nodes(str(pbf_filepath))
    print(
        f"ID collection complete. Found {len(id_handler.required_ways)} ways and {len(id_handler.required_nodes)} nodes for buildings.")

    try:
        print(f"Fetching boundary for {place_name}.")
        boundary_gdf = ox.geocode_to_gdf(place_name)
        boundary_geom = boundary_gdf.geometry.iloc[0]
        print("Boundary fetched successfully.")
    except Exception as e:
        print(f"Could not fetch boundary for {place_name}. Error: {e}. Aborting benchmark.")
        return

    cold_start_time = None
    hot_start_times = []
    last_successful_gdf = None

    # Cold start run
    print("\nRunning Cold Start (First run).")
    try:
        with Timer() as t:
            builder = BuildingGeometryHandler(id_handler.required_nodes, id_handler.required_ways)
            builder.apply_file(str(pbf_filepath), locations=True)
            all_buildings_gdf = builder.get_geodataframe()
            final_gdf = all_buildings_gdf[all_buildings_gdf.intersects(boundary_geom)]

        cold_start_time = t.interval
        last_successful_gdf = final_gdf
        print(f"Cold start completed in {cold_start_time:.4f}s. Found {len(final_gdf)} features.")
    except Exception as e:
        print(f"Cold start run failed. Error: {e}. Aborting subsequent runs.")
        return

    # Hot start runs
    if num_runs > 1:
        print("\nRunning Hot Starts (Second to last run).")
        for i in range(num_runs - 1):
            with Timer() as t:
                builder = BuildingGeometryHandler(id_handler.required_nodes, id_handler.required_ways)
                builder.apply_file(str(pbf_filepath), locations=True)
                all_buildings_gdf = builder.get_geodataframe()
                _ = all_buildings_gdf[all_buildings_gdf.intersects(boundary_geom)]

            hot_start_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # After the loop, calculate and save results (this part is unchanged as requested)
        print("Benchmark run summary:")
        if cold_start_time is not None:
            num_features = len(last_successful_gdf)
            output_filename = f"{place_name_clean.lower()}_buildings_pyosmium.geoparquet"
            output_path = PROCESSED_DATA_DIR / 'geopandas_generated' / output_filename
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
            last_successful_gdf.to_parquet(output_path)
            # Get file size
            output_size_bytes = output_path.stat().st_size
            output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

            cold_result = {
                'use_case': '1&2. Ingestion & Filtering (OSM Data)',
                'technology': 'PyOsmium + GeoPandas',
                'operation_description': f'Extract {place_name_clean} buildings from PBF',
                'test_dataset': pbf_filepath.name,
                'execution_time_s': cold_start_time,
                'num_runs': 1,
                'output_size_mb': output_size_mb,
                'notes': f'Found {num_features} buildings. Cold start (first run).'
            }
            save_results(cold_result)
            print(f"Cold start result saved. Time: {cold_start_time:.4f}s, Features: {num_features}.")

        if len(hot_start_times) > 0:
            average_hot_time = sum(hot_start_times) / len(hot_start_times)
            hot_result = {
                'use_case': '1&2. Ingestion & Filtering (OSM Data)',
                'technology': 'PyOsmium + GeoPandas',
                'operation_description': f'Extract {place_name_clean} buildings from PBF',
                'test_dataset': pbf_filepath.name,
                'execution_time_s': average_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': output_size_mb,
                'notes': f'Found {num_features} buildings. Average of {len(hot_start_times)} hot cache runs.'
            }
            save_results(hot_result)
            print(f"Average hot start time: {average_hot_time:.4f}s over {len(hot_start_times)} runs.")
            print("Hot start average result saved.")
        else:
            print("No successful runs to save.")


if __name__ == '__main__':
    PLACE_TO_BENCHMARK = "Milan, Italy"
    NUMBER_OF_RUNS = 50 # The number of runs will be changed to 100 once the time is enough to run them all
    PBF_FILEPATH = RAW_DATA_DIR / 'lombardy-latest.osm.pbf'

    run_pyosmium_ingestion_and_filtering(place_name=PLACE_TO_BENCHMARK, pbf_filepath=PBF_FILEPATH, num_runs=NUMBER_OF_RUNS)
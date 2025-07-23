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
PBF_FILEPATH = RAW_DATA_DIR / 'lombardy-latest.osm.pbf'

# This handler processes OSM PBF data to extract building geometries, including complex multipolygons (relations).
class BuildingsHandler(o.SimpleHandler):
    def __init__(self):
        super(BuildingsHandler, self).__init__()
        self.nodes_cache = {}
        self.ways_cache = {}
        self.buildings = []

    def node(self, n):
        if 'building' in n.tags:
            self.nodes_cache[n.id] = (n.location.lon, n.location.lat)

    def way(self, w):
        if 'building' in w.tags:
            self.ways_cache[w.id] = {'nodes': [n.ref for n in w.nodes], 'tags': dict(w.tags)}

    def relation(self, r):
        if r.tags.get('type') == 'multipolygon' and 'building' in r.tags:
            try:
                outer_rings = []
                inner_rings = []
                for member in r.members:
                    if member.type == 'w':
                        way = self.ways_cache.get(member.ref)
                        if way:
                            points = [self.nodes_cache[node_id] for node_id in way['nodes'] if
                                      node_id in self.nodes_cache]
                            if len(points) > 2:
                                if member.role == 'outer':
                                    outer_rings.append(points)
                                elif member.role == 'inner':
                                    inner_rings.append(points)

                if outer_rings:
                    polygon = MultiPolygon([Polygon(outer) for outer in outer_rings]) if len(
                        outer_rings) > 1 else Polygon(outer_rings[0])
                    if inner_rings:
                        for inner in inner_rings:
                            polygon = polygon.difference(Polygon(inner))

                    self.buildings.append({'geometry': polygon, 'tags': dict(r.tags)})
            except Exception:
                pass  # Ignore malformed relations

    # It converts the collected building data into a GeoDataFrame.
    def get_geodataframe(self):
        """Builds a GeoDataFrame from the processed ways and relations."""
        # First, process simple ways that were not part of a relation
        for way_id, way_data in self.ways_cache.items():
            try:
                points = [self.nodes_cache[node_id] for node_id in way_data['nodes'] if node_id in self.nodes_cache]
                if len(points) > 2:
                    self.buildings.append({'geometry': Polygon(points), 'tags': way_data['tags']})
            except Exception:
                pass  # Ignore malformed ways

        if not self.buildings:
            return gpd.GeoDataFrame([], columns=['geometry', 'tags'], crs="EPSG:4326")

        return gpd.GeoDataFrame(self.buildings, crs="EPSG:4326")

def run_pyosmium_ingestion_and_filtering(place_name, num_runs=100):
    """
    Runs the data ingestion and filtering benchmark using PyOsmium and GeoPandas, with a final filter,
    separating cold and hot runs.
    """
    place_name_clean = place_name.split(',')[0]
    print(f"Testing PyOsmium + GeoPandas ingestion & filtering for {place_name_clean} buildings over {num_runs} runs.")

    if not PBF_FILEPATH.exists():
        print(f"ERROR: PBF file not found at {PBF_FILEPATH}. Aborting.")
        return

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
            handler = BuildingsHandler()
            handler.apply_file(str(PBF_FILEPATH), locations=True)
            all_buildings_gdf = handler.get_geodataframe()
            final_gdf = all_buildings_gdf[all_buildings_gdf.intersects(boundary_geom)]
        cold_start_time = t.interval
        last_successful_gdf = final_gdf
        print(f"Cold start completed in {cold_start_time:.4f}s. Found {len(final_gdf)} features.")
    except Exception as e:
        print(f"Cold start run failed. Error: {e}. Aborting subsequent runs.")
        return

    # Hot start runs
    if num_runs > 1:
        print("\n--- Running Hot Starts ---")
        for i in range(num_runs - 1):
            with Timer() as t:
                handler = BuildingsHandler()
                handler.apply_file(str(PBF_FILEPATH), locations=True)
                all_buildings_gdf = handler.get_geodataframe()
                _ = all_buildings_gdf[all_buildings_gdf.intersects(boundary_geom)]
            hot_start_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # After the loop, calculate and save results
        print("Benchmark run summary:")
        # Process and save cold start result
        if cold_start_time is not None:
            num_features = len(last_successful_gdf)
            output_filename = f"{place_name_clean.lower()}_buildings_duckdb.geoparquet"
            output_path = PROCESSED_DATA_DIR / output_filename
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
            last_successful_gdf.to_parquet(output_path)
            output_size_mb = 'N/A'  # Size calculation is optional here

            # Save cold start result
            cold_result = {
                'use_case': '1&2. Ingestion & Filtering (OSM Data)',
                'technology': 'PyOsmium + GeoPandas',
                'operation_description': f'Extract {place_name_clean} buildings from PBF',
                'test_dataset': PBF_FILEPATH.name,
                'execution_time_s': cold_start_time,
                'num_runs': 1,
                'output_size_mb': output_size_mb,
                'notes': f'Found {num_features} buildings. Cold start (first run).'
            }
            save_results(cold_result)
            print(f"Cold start result saved. Time: {cold_start_time:.4f}s, Features: {num_features}.")

        # Process and save hot start results
        if len(hot_start_times) > 0:
            average_hot_time = sum(hot_start_times) / len(hot_start_times)
            hot_result = {
                'use_case': '1&2. Ingestion & Filtering (OSM Data)',
                'technology': 'PyOsmium + GeoPandas',
                'operation_description': f'Extract {place_name_clean} buildings from PBF',
                'test_dataset': PBF_FILEPATH.name,
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
    # This is the only variable you can to change to test a different place.
    PLACE_TO_BENCHMARK = "Milan, Italy"
    NUMBER_OF_RUNS = 100

    run_pyosmium_ingestion_and_filtering(place_name=PLACE_TO_BENCHMARK, num_runs=NUMBER_OF_RUNS)
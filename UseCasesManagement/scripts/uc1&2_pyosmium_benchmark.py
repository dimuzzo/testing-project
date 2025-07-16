import osmium
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Polygon
from pathlib import Path
import sys

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils import Timer, save_results

class BuildingHandler(osmium.SimpleHandler):
    """
    Osmium handler to process OSM ways and relations to build all building polygons.
    """
    def __init__(self):
        super(BuildingHandler, self).__init__()
        self.ways_cache = {}
        self.buildings = []

    def way(self, w):
        # This method caches all closed ways and identifies simple buildings.
        if w.is_closed() and len(w.nodes) > 2:
            try:
                coords = [(n.lon, n.lat) for n in w.nodes]
                polygon = Polygon(coords)
                self.ways_cache[w.id] = polygon

                if 'building' in w.tags:
                    self.buildings.append({
                        'osm_id': f'way/{w.id}',
                        'geometry': polygon,
                        'name': w.tags.get('name', None)
                    })
            except osmium.InvalidLocationError:
                pass

    def relation(self, r):
        # This method assembles multipolygon buildings from cached ways.
        if 'building' in r.tags and r.tags.get('type') == 'multipolygon':
            try:
                outer_rings = [self.ways_cache[m.ref] for m in r.members if
                               m.role == 'outer' and m.ref in self.ways_cache]
                inner_rings = [self.ways_cache[m.ref] for m in r.members if
                               m.role == 'inner' and m.ref in self.ways_cache]

                if not outer_rings:
                    return

                polygon_with_holes = Polygon(outer_rings[0].exterior.coords, [h.exterior.coords for h in inner_rings])

                self.buildings.append({
                    'osm_id': f'relation/{r.id}',
                    'geometry': polygon_with_holes,
                    'name': r.tags.get('name', None)
                })
            except Exception:
                pass

def run_pyosmium_ingestion(pbf_filename, filter_place_name):
    """
    Runs the data ingestion benchmark using PyOsmium and GeoPandas, with a final filter.
    """
    print(f"Testing PyOsmium + GeoPandas ingestion & filtering for {filter_place_name}.")

    current_script_path = Path(__file__).resolve()
    working_root = current_script_path.parent.parent
    pbf_path = working_root / 'data' / 'raw' / pbf_filename

    with Timer() as t:
        # SProcess the PBF file with the handler
        handler = BuildingHandler()
        print(f"Reading PBF file {pbf_path.name} with PyOsmium.")
        handler.apply_file(str(pbf_path), locations=True)

        # Create the full GeoDataFrame
        total_buildings_in_pbf = len(handler.buildings)
        print(f"Found {total_buildings_in_pbf} total buildings (ways & relations). Creating GeoDataFrame.")
        gdf_full = gpd.GeoDataFrame(handler.buildings, crs="EPSG:4326")

        # Clean and filter the data
        print("Cleaning and filtering geometries.")
        gdf_full['geometry'] = gdf_full.geometry.buffer(0)
        boundary_gdf = ox.geocode_to_gdf(filter_place_name)
        gdf_filtered = gpd.sjoin(gdf_full, boundary_gdf, how='inner', predicate='intersects')

        print(f"Filtered down to {len(gdf_filtered)} buildings for {filter_place_name}.")

    print(f"PyOsmium ingestion & filtering completed in {t.interval:.4f} seconds.")

    result_data = {
        'use_case': '1. Ingestion & 2. Filtering (OSM Data)',
        'technology': 'PyOsmium + GeoPandas',
        'operation_description': f'Read PBF (ways+relations) and filter for {filter_place_name}',
        'test_dataset': pbf_path.name,
        'execution_time_s': t.interval,
        'output_size_mb': 'N/A',
        'notes': f'Processed {total_buildings_in_pbf} features, filtered to {len(gdf_filtered)} for target area.'
    }
    save_results(result_data)

if __name__ == '__main__':
    PBF_FILE = 'lombardy-latest.osm.pbf'
    PLACE_TO_FILTER = 'Milan, Italy'

    run_pyosmium_ingestion(
        pbf_filename=PBF_FILE,
        filter_place_name=PLACE_TO_FILTER
    )
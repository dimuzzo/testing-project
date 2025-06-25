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
    A custom osmium handler to process OSM ways and build building polygons.
    """
    def __init__(self):
        super(BuildingHandler, self).__init__()
        self.buildings = []

    def way(self, w):
        # This method is called for every 'way' in the PBF file
        if 'building' in w.tags and w.is_closed() and len(w.nodes) > 2:
            try:
                # Reconstruct the polygon geometry from the list of nodes.
                # This is possible because we will use `locations=True` when applying the file,
                # which makes osmium pre-cache node locations.
                coords = [(n.lon, n.lat) for n in w.nodes]
                polygon = Polygon(coords)

                # Store the data for the building
                self.buildings.append({
                    'osm_id': w.id,
                    'geometry': polygon,
                    'name': w.tags.get('name', None) # Get the name if it exists
                })
            except osmium.InvalidLocationError:
                # Ignore ways where node locations are not available
                pass

def run_pyosmium_ingestion(pbf_filename, filter_place_name):
    """
    Runs the data ingestion benchmark using PyOsmium and GeoPandas.
    """
    print(f"Testing: PyOsmium + GeoPandas, filtering for {filter_place_name}")

    current_script_path = Path(__file__).resolve()
    working_root = current_script_path.parent.parent
    pbf_path = working_root / 'data' / 'raw' / pbf_filename

    total_buildings_in_pbf = 0

    with Timer() as t:
        # Instantiate the handler
        handler = BuildingHandler()

        # Apply the handler to the PBF file.
        # `locations=True` is crucial: it tells Osmium to build an index
        # of node locations in memory so we can create geometries.
        # This is the most memory-intensive part.
        print(f"Reading PBF file {pbf_path.name} with PyOsmium...")
        handler.apply_file(str(pbf_path), locations=True)
        total_buildings_in_pbf = len(handler.buildings)
        print(f"Found {total_buildings_in_pbf} total buildings. Creating initial GeoDataFrame...")
        gdf_full = gpd.GeoDataFrame(handler.buildings, crs="EPSG:4326")

        # Filter the in-memory GeoDataFrame by the place boundary
        print(f"Filtering in-memory GeoDataFrame for {filter_place_name}...")
        boundary_gdf = ox.geocode_to_gdf(filter_place_name)
        # Use gpd.clip to keep only buildings within the boundary
        gdf_filtered = gpd.clip(gdf_full, boundary_gdf)
        print(f"Filtered down to {len(gdf_filtered)} buildings for {filter_place_name}.")

    print(f"PyOsmium ingestion+filtering completed in {t.interval:.4f} seconds.")

    # For this test, 'output_size_mb' is not directly applicable
    # as we are just creating an in-memory GeoDataFrame.
    # A more advanced test could measure peak memory usage.
    result_data = {
        'use_case': '1. Ingestion & Filtering',
        'technology': 'PyOsmium + GeoPandas',
        'operation_description': f'Read PBF and filter for {filter_place_name}',
        'test_dataset': pbf_path.name,
        'execution_time_s': t.interval,
        'output_size_mb': 'N/A',
        'notes': f'Read {total_buildings_in_pbf} buildings from PBF, then filtered to {len(gdf_filtered)} for the target area.'
    }
    save_results(result_data)

if __name__ == '__main__':
    # IMPORTANT: This approach reads the ENTIRE PBF file into memory.
    # It does not filter by area beforehand like QuackOSM or PostGIS.
    # It is recommended to run this on a smaller PBF file first.
    PBF_FILE = 'lombardy-latest.osm.pbf'
    # But we want to get the final count only for Milan
    PLACE_TO_FILTER = 'Milan, Italy'

    run_pyosmium_ingestion(
        pbf_filename=PBF_FILE,
        filter_place_name=PLACE_TO_FILTER
    )
import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path
import sys

# Assumes benchmark_utils.py is in the parent directory of this script's location
sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_utils import Timer

# Database connection string
# Format: "postgresql://user:password@host:port/dbname"
DB_CONNECTION_URL = "postgresql://postgres:postgres@localhost:5432/osm_benchmark_db"

# Path to processed data
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

# List of files to load
files_to_load = [
    # Pinerolo
    'pinerolo_buildings.geoparquet',
    'pinerolo_restaurants.geoparquet',
    'pinerolo_bus_stops.geoparquet',
    # Milan
    'milan_buildings.geoparquet',
    'milan_restaurants.geoparquet',
    'milan_bus_stops.geoparquet',
    # Rome
    'rome_buildings.geoparquet',
    'rome_restaurants.geoparquet',
    'rome_bus_stops.geoparquet'
]

def main():
    """
    Connects to the PostGIS database and loads all specified GeoParquet files,
    timing each operation.
    """
    print(f"Connecting to the database.")
    engine = create_engine(DB_CONNECTION_URL)

    with Timer() as total_timer:
        for filename in files_to_load:
            file_path = PROCESSED_DATA_DIR / filename
            # Table name is derived from the filename (e.g., "pinerolo_buildings")
            table_name = file_path.stem

            if not file_path.exists():
                print(f"WARNING: File not found, skipping {filename}.")
                continue

            print(f"\nLoading {filename} into the table {table_name}.")

            try:
                # Read the GeoParquet file
                gdf = gpd.read_parquet(file_path)
                gdf.crs = "EPSG:4326"

                with Timer() as t:
                    # Write the GeoDataFrame to PostGIS
                    # 'if_exists="replace"' will drop the table if it already exists and create a new one.
                    # A spatial index is created automatically on the 'geometry' column.
                    gdf.to_postgis(
                        name=table_name,
                        con=engine,
                        if_exists="replace",
                        index=True,
                        index_label="id", # Use a specific name for the primary key
                    )

                print(f"Successfully loaded {len(gdf)} features into {table_name} in {t.interval:.2f} seconds.")

            except Exception as e:
                print(f"ERROR: Failed to load {filename}. Reason: {e}.")

    print(f"\nData loading process complete. Total time: {total_timer.interval:.2f} seconds.")

if __name__ == '__main__':
    main()
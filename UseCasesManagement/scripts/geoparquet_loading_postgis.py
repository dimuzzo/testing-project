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
cities = ['pinerolo', 'milan', 'rome']

features_to_load = [
    'buildings',
    'restaurants',
    'bus_stops',
    'neighborhoods',
    'parks',
    'hospitals',
    'residential_streets',
    'trees'
]

def main():
    """
    Connects to the PostGIS database and loads all specified GeoParquet files,
    timing each operation.
    """
    print(f"Connecting to the database.")
    engine = create_engine(DB_CONNECTION_URL)

    with Timer() as total_timer:
        # Iterates through each city and feature type to load the data.
        for city in cities:
            for feature in features_to_load:
                input_filename = f"{city}_{feature}.geoparquet"
                table_name = f"{city}_{feature}"  # The table will be named like 'milan_parks'
                input_filepath = PROCESSED_DATA_DIR / input_filename

                if not input_filepath.exists():
                    print(f"WARNING: File not found, skipping {input_filename}.")
                    continue

                print(f"\nLoading {input_filename} into the table {table_name}.")

                try:
                    # Read the GeoParquet file
                    gdf = gpd.read_parquet(input_filepath)
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
                    print(f"ERROR: Failed to load {input_filename}. Reason: {e}.")

    print(f"\nData loading process complete. Total time: {total_timer.interval:.2f} seconds.")

if __name__ == '__main__':
    main()
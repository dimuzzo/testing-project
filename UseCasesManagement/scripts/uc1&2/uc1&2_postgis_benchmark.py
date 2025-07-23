import psycopg2
import osmnx as ox
from pathlib import Path
import sys

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# DB Connection Details
DB_NAME = "osm_benchmark_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
RAW_DATA_DIR = WORKING_ROOT / 'data' / 'raw'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'
PBF_FILEPATH = RAW_DATA_DIR / 'italy-latest.osm.pbf'

def run_postgis_extraction(place_name, num_runs=100):
    """
    Runs a repeated benchmark for a PostGIS query, separating cold and hot runs.
    """
    place_name_clean = place_name.split(',')[0]
    print(f"Testing PostGIS extraction (filtering) for {place_name_clean} buildings over {num_runs} runs.")

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )

        print(f"Fetching boundary for {place_name}.")
        boundary_gdf = ox.geocode_to_gdf(place_name)
        boundary_wkt = boundary_gdf.geometry.iloc[0].wkt

        # The 'way' column in the DB is in SRID 3857 (Web Mercator).
        # Our input geometry from osmnx is in SRID 4326 (WGS84).
        # We must transform our input geometry to match the database's CRS.
        query = f"""
        SELECT ST_AsBinary(way)
        FROM planet_osm_polygon
        WHERE building IS NOT NULL 
        AND ST_Intersects(
            way, 
            ST_Transform(ST_SetSRID(ST_GeomFromText('{boundary_wkt}'), 4326), 3857)
        );
        """

        cold_start_time = None
        hot_start_times = []
        num_features = 0

        # Cold start run
        print("\nRunning Cold Start (First run).")
        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        cold_start_time = t.interval
        num_features = len(results)
        print(f"Cold start completed in {cold_start_time:.4f}s. Found {num_features} features.")

        # Hot start runs
        if num_runs > 1:
            print("\nRunning Hot Starts (Second to last run).")
            for i in range(num_runs - 1):
                with Timer() as t:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    _ = cursor.fetchall()
                    cursor.close()
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            print("\n")

        # For this test, 'output_size_mb' is not directly applicable
        # as we are using an external DataBase on Postgresql.

        # Save cold start result
        cold_result  = {
            'use_case': '1&2. Filtering (OSM Data)',
            'technology': 'PostGIS',
            'operation_description': f'Extract {place_name_clean} buildings from DB',
            'test_dataset': PBF_FILEPATH.name,
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': 'N/A',
            'notes': f'Found {num_features} buildings. Cold start query time.'
        }
        save_results(cold_result)
        print("Cold start result saved.")

        # Save hot start average result
        if hot_start_times:
            average_hot_time = sum(hot_start_times) / len(hot_start_times)
            hot_result  = {
                'use_case': '1&2. Filtering (OSM Data)',
                'technology': 'PostGIS',
                'operation_description': f'Extract {place_name_clean} buildings from DB',
                'test_dataset': PBF_FILEPATH.name,
                'execution_time_s': average_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': 'N/A',
                'notes': f'Found {num_features} buildings. Average of {len(hot_start_times)} hot cache query times.'
            }
            save_results(hot_result)
            print(f"Average hot start time: {average_hot_time:.4f}s.")
            print("Hot start average result saved.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # This is the only variable you can to change to test a different place.
    PLACE_TO_BENCHMARK = "Milan, Italy"
    NUMBER_OF_RUNS = 100

    run_postgis_extraction(place_name=PLACE_TO_BENCHMARK, num_runs=NUMBER_OF_RUNS)
import psycopg2
import osmnx as ox
from pathlib import Path
from utils import Timer, save_results

# DB Connection Details
DB_NAME = "osm_benchmark_db"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

def run_postgis_extraction(place_name, pbf_filename):
    print(f"Testing PostGIS extraction for {place_name}")
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )

        area_gdf = ox.geocode_to_gdf(place_name)
        area_wkt = area_gdf.geometry.iloc[0].wkt

        # The 'way' column in the DB is in SRID 3857 (Web Mercator).
        # Our input geometry from osmnx is in SRID 4326 (WGS84).
        # We must transform our input geometry to match the database's CRS.
        sql_query = f"""
        SELECT count(*)
        FROM planet_osm_polygon
        WHERE building IS NOT NULL 
        AND ST_Intersects(
            way, 
            ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), 3857)
        );
        """

        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            record_count = cursor.fetchone()[0]
            cursor.close()

        print(f"PostGIS query for {place_name} completed in {t.interval:.4f} seconds.")
        print(f"Found {record_count} buildings.")

        # For this test, 'output_size_mb' is not directly applicable
        # as we are using an external DataBase on Postgresql.
        result_data = {
            'use_case': '1. Ingestion & 2. Filtering (OSM Data)',
            'technology': 'PostGIS',
            'operation_description': f'Extract buildings query for {place_name}',
            'test_dataset': pbf_filename,
            'execution_time_s': t.interval,
            'output_size_mb': 'N/A',
            'notes': f'Query Time only. Found {record_count} buildings.'
        }
        save_results(result_data)

    except psycopg2.OperationalError as e:
        print(f"SKIPPING PostGIS test: Could not connect to database. Please check connection details and ensure the server is running.")
        print(f"Error: {e}.")
    except psycopg2.errors.UndefinedTable:
        print(f"SKIPPING PostGIS test: Table 'planet_osm_polygon' not found.")
        print(f"Please run the 'osm2pgsql' command first to import the data.")
    except Exception as e:
        print(f"An error occurred: {e}.")
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    # You can change the place here to test other cities like Milan
    run_postgis_extraction(
        place_name="Milan, Italy",
        pbf_filename="italy-latest.osm.pbf"
    )
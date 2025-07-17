import geopandas as gpd
import duckdb
import psycopg2
from pathlib import Path
import sys
import os

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
SHAPEFILE_PATH = WORKING_ROOT / 'data' / 'raw' / 'comuni_istat' / 'Com01012025_WGS84.shp'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

def run_duckdb_benchmark():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    # Use Case 1: Ingestion
    print("\nTesting DuckDB Spatial ingestion for vector data.")
    ingestion_query = f"CREATE OR REPLACE TABLE comuni AS SELECT * FROM ST_Read('{str(SHAPEFILE_PATH).replace('\\', '/')}');"
    with Timer() as t:
        con.execute(ingestion_query)
    feature_count = con.execute("SELECT COUNT(*) FROM comuni;").fetchone()[0]
    print(f"DuckDB loaded {feature_count} features in {t.interval:.4f}s.")

    save_results({
        'use_case': '1. Ingestion (Vector Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Load Shapefile using ST_Read',
        'test_dataset': SHAPEFILE_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': 'N/A',
        'notes': f'Loaded {feature_count} polygons into in-memory table.'
    })

    # Use Case 2: Filtering
    print("\nTesting DuckDB Spatial filtering for vector data.")
    # Must convert the geometries in WKB format or the output size can't be measured
    filtering_query = "SELECT *, ST_AsWKB(geom) as geom_wkb FROM comuni WHERE COD_REG = 1;"
    with Timer() as t:
        result_df = con.execute(filtering_query).df()
    print(f"DuckDB filtered to {len(result_df)} features in {t.interval:.4f}s.")

    output_path = PROCESSED_DATA_DIR / 'comuni_piemonte_duckdb.geoparquet'
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    # Create the correct GeoDataFrame, dropping temporary geometry columns ('geom' and 'geom_wkb')
    # after creating the proper 'geometry' series.
    result_gdf = gpd.GeoDataFrame(
        result_df.drop(columns=['geom', 'geom_wkb']),
        geometry=gpd.GeoSeries.from_wkb(result_df['geom_wkb'].apply(bytes)),
        crs="EPSG:4326"
    )
    result_gdf.to_parquet(output_path)
    output_size_bytes = output_path.stat().st_size
    output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

    save_results({
        'use_case': '2. Filtering (Vector Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Filter municipalities by region code from memory',
        'test_dataset': SHAPEFILE_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': output_size_mb,
        'notes': f'Filtered for cod_reg == 1.'
    })

    con.close()

def run_postgis_benchmark():
    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')
        # Use Case 2: Filtering
        print("\nTesting PostGIS filtering for vector data.")
        # The table 'comuni_istat' should have been created during the manual import
        query = "SELECT * FROM comuni_istat WHERE cod_reg = 1;"
        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

        print(f"PostGIS filtered to {len(results)} features in {t.interval:.4f}s.")
        save_results({
            'use_case': '2. Filtering (Vector Data)',
            'technology': 'PostGIS',
            'operation_description': 'Filter municipalities by region code from DB table',
            'test_dataset': SHAPEFILE_PATH.name,
            'execution_time_s': t.interval,
            'output_size_mb': 'N/A',
            'notes': 'Query Time only. Data should be pre-loaded with shp2pgsql.'
        })

    except psycopg2.OperationalError as e:
        print("SKIPPING PostGIS test: Could not connect to database. Please check connection details and ensure the server is running.")
        print(f"Error: {e}.")
    except psycopg2.errors.UndefinedTable:
        print(f"SKIPPING PostGIS test: Table 'comuni_istat' not found.")
        print(f"Please run the 'shp2pgsql' command first to import the data.")
    except Exception as e:
        print(f"An error occurred during PostGIS test: {e}.")
    finally:
        if conn:
            conn.close()

def run_geopandas_benchmark():
    # Use Case 1: Ingestion
    print("\nTesting GeoPandas ingestion for vector data.")
    with Timer() as t:
        gdf = gpd.read_file(SHAPEFILE_PATH)
    print(f"GeoPandas loaded {len(gdf)} features in {t.interval:.4f}s.")

    save_results({
        'use_case': '1. Ingestion (Vector Data)',
        'technology': 'GeoPandas',
        'operation_description': 'Load Shapefile into GeoDataFrame',
        'test_dataset': SHAPEFILE_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': 'N/A',
        'notes': f'Loaded {len(gdf)} polygons into memory.'
    })

    # Use Case 2: Filtering
    print("\nTesting GeoPandas filtering for vector data.")
    with Timer() as t:
        # Filter for municipalities in the province of Turin (COD_PROV = 1)
        piedmont_gdf = gdf[gdf['COD_REG'] == 1]
    print(f"GeoPandas filtered to {len(piedmont_gdf)} features in {t.interval:.4f}s.")

    output_path = PROCESSED_DATA_DIR / 'comuni_piemonte_geopandas.geoparquet'
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    piedmont_gdf.to_parquet(output_path)
    output_size_bytes = output_path.stat().st_size
    output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

    save_results({
        'use_case': '2. Filtering (Vector Data)',
        'technology': 'GeoPandas',
        'operation_description': 'Filter municipalities by region code from memory',
        'test_dataset': SHAPEFILE_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': output_size_mb,
        'notes': f'Filtered for cod_reg == 1.'
    })

if __name__ == '__main__':
    print(f"Running Vector Data Benchmark.")
    print(f"Using input file: {SHAPEFILE_PATH.name}.")

    if not SHAPEFILE_PATH.exists():
        print(f"\nERROR: Input file not found at {SHAPEFILE_PATH.name}.")
        print("Please download the correct ISTAT administrative boundaries and place them in the correct folder.")
    else:
        run_duckdb_benchmark()
        run_postgis_benchmark()
        run_geopandas_benchmark()
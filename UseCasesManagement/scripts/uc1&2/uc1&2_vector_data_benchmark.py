import geopandas as gpd
import duckdb
import psycopg2
from pathlib import Path
import sys

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
RAW_DATA_DIR = WORKING_ROOT / 'data' / 'raw'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'
SHAPEFILE_INPUT = RAW_DATA_DIR / 'comuni_istat' / 'Com01012025_WGS84.shp'

# Ensure the output directory exists
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def benchmark_duckdb_vector(shapefile_path, num_runs=100):
    """
    Runs Ingestion and Filtering benchmarks for DuckDB on a pure vector file.
    """
    print("\nTesting DuckDB Spatial ingestion & filtering for Pure Vector Data.")

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    print("\nRunning DuckDB Spatial Ingestion (ST_Read).")

    # Cold start run
    with Timer() as t:
        con.execute(f"CREATE OR REPLACE TABLE comuni AS SELECT * FROM ST_Read('{str(shapefile_path).replace('\\', '/')}');")
    cold_start_time = t.interval

    # Get feature count
    num_features = con.execute("SELECT COUNT(*) FROM comuni;").fetchone()[0]
    print(f"Cold start completed in {cold_start_time:.4f}s. Found {num_features} features.")

    # Save cold start result
    save_results({
        'use_case': '1. Ingestion (Vector Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Read Shapefile using ST_Read',
        'test_dataset': shapefile_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': f'Found {num_features} features. Cold start (first run).'
    })

    # Hot start runs
    hot_ingestion_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            try:
                with Timer() as t:
                    con.execute(
                        f"CREATE OR REPLACE TABLE comuni AS SELECT * FROM ST_Read('{str(shapefile_path).replace('\\', '/')}');")
                hot_ingestion_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            except Exception as e:
                print(f"\nHot run {i + 2}/{num_runs} failed. Error: {e}")
        print("\n")  # Newline after progress indicator

        # Save hot start results
        avg_hot_time = sum(hot_ingestion_times) / len(hot_ingestion_times)
        save_results({
            'use_case': '1. Ingestion (Vector Data)',
            'technology': 'DuckDB Spatial',
            'operation_description': 'Read Shapefile using ST_Read',
            'test_dataset': shapefile_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_ingestion_times),
            'output_size_mb': 'N/A',
            'notes': f'Found {num_features} features. Average of {len(hot_ingestion_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_ingestion_times)} runs.")
        print("Hot start average result saved.")

    print("\nRunning DuckDB Spatial Filtering (SQL Query).")

    # Cold start run
    # The geometry must be converted to Well-Known Binary (WKB) format to measure the output size.
    # This is a critical step for interoperability, allowing other libraries
    # like GeoPandas to correctly parse the geometry from the resulting DataFrame.
    filtering_query = "SELECT *, ST_AsWKB(geom) as geom_wkb FROM comuni WHERE COD_REG = 1;"
    with Timer() as t:
        result_df = con.execute(filtering_query).df()
    cold_start_time = t.interval
    num_filtered_features = len(result_df)
    print(f"Cold start completed in {cold_start_time:.4f}s. Filtered to {num_filtered_features} features.")

    # Reconstruct the GeoDataFrame from the Pandas DataFrame.
    # Start by dropping temporary geometry columns ('geom' and 'geom_wkb').
    # The gpd.GeoSeries.from_wkb function is the key to this process,
    # deserializing the WKB byte strings back into Shapely geometry objects
    result_gdf = gpd.GeoDataFrame(
        result_df.drop(columns=['geom', 'geom_wkb']),
        geometry=gpd.GeoSeries.from_wkb(result_df['geom_wkb'].apply(bytes)),
        crs="EPSG:4326"
    )
    output_filename = 'comuni_filtered_duckdb.geoparquet'
    output_path = PROCESSED_DATA_DIR / 'duckdb_generated' / output_filename
    result_gdf.to_parquet(output_path)
    output_size_bytes = output_path.stat().st_size
    output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

    # Save cold start result
    save_results({
        'use_case': '2. Filtering (Vector Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Filter table by attribute',
        'test_dataset': shapefile_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': output_size_mb,
        'notes': f'Filtered to {num_filtered_features} features. Cold start (first run).'
    })

    # Hot start runs
    hot_filtering_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            try:
                with Timer() as t:
                    _ = con.execute(filtering_query).df()
                hot_filtering_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            except Exception as e:
                print(f"\nHot run {i + 2}/{num_runs} failed. Error: {e}")
        print("\n")  # Newline after progress indicator

        # Save hot start results
        avg_hot_time = sum(hot_filtering_times) / len(hot_filtering_times)
        save_results({
            'use_case': '2. Filtering (Vector Data)',
            'technology': 'DuckDB Spatial',
            'operation_description': 'Filter table by attribute',
            'test_dataset': shapefile_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_filtering_times),
            'output_size_mb': output_size_mb,
            'notes': f'Filtered to {num_filtered_features} features. Average of {len(hot_filtering_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_filtering_times)} runs.")
        print("Hot start average result saved.")

def benchmark_postgis_vector(num_runs=100):
    """
    Runs Filtering benchmarks for PostGIS on a pure vector table.
    NOTE: Ingestion must be performed and timed manually as a one-time setup cost,
    using shp2pgsql tool.
    """
    print("\nTesting PostGIS filtering for Pure Vector Data.")

    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')
        print("\nRunning PostGIS Filtering (SQL Query).")

        # A standard SQL attribute filter. Performance depends on the PostgreSQL query optimizer,
        # table statistics and the presence of a database index on the 'cod_reg' column.
        query = "SELECT * FROM comuni_istat WHERE cod_reg = 1;"

        # Cold start run
        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        cold_start_time = t.interval
        num_features = len(results)
        print(f"Cold start completed in {cold_start_time:.4f}s. Found {num_features} features.")

        # For this test, 'output_size_mb' is not directly applicable
        # as we are using an external DataBase on Postgresql.

        # Save cold start result
        save_results({
            'use_case': '2. Filtering (Vector Data)',
            'technology': 'PostGIS',
            'operation_description': 'Filter table by attribute',
            'test_dataset': 'comuni_istat',
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': 'N/A',
            'notes': f'Found {num_features} features. Cold start query time.'
        })

        # Hot start runs
        hot_filtering_times = []
        if num_runs > 1:
            for i in range(num_runs - 1):
                with Timer() as t:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    _ = cursor.fetchall()
                    cursor.close()
                hot_filtering_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            print("\n")

            # Save hot start results
            avg_hot_time = sum(hot_filtering_times) / len(hot_filtering_times)
            save_results({
                'use_case': '2. Filtering (Vector Data)',
                'technology': 'PostGIS',
                'operation_description': 'Filter table by attribute',
                'test_dataset': 'comuni_istat',
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_filtering_times),
                'output_size_mb': 'N/A',
                'notes': f'Found {num_features} features. Average of {len(hot_filtering_times)} hot cache query times.'
            })
            print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_filtering_times)} runs.")

    except Exception as e:
        print(f"An error occurred during PostGIS test: {e}.")
    finally:
        if conn:
            conn.close()

def benchmark_geopandas_vector(shapefile_path, num_runs=100):
    """
    Runs Ingestion and Filtering benchmarks for GeoPandas on a pure vector file.
    """
    print("\nTesting GeoPandas ingestion & filtering for Pure Vector Data.")

    print("\nRunning GeoPandas Ingestion (gpd.read_file).")

    # Cold start run
    with Timer() as t:
        gdf = gpd.read_file(shapefile_path)
    cold_start_time = t.interval
    num_features = len(gdf)
    print(f"Cold start completed in {cold_start_time:.4f}s. Found {num_features} features.")

    # Save cold start result
    save_results({
        'use_case': '1. Ingestion (Vector Data)',
        'technology': 'GeoPandas',
        'operation_description': 'Read Shapefile into GeoDataFrame',
        'test_dataset': shapefile_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': f'Found {num_features} features. Cold start (first run).'
    })

    # Hot start runs
    hot_ingestion_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            with Timer() as t:
                _ = gpd.read_file(shapefile_path)
            hot_ingestion_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # Save hot start results
        avg_hot_time = sum(hot_ingestion_times) / len(hot_ingestion_times)
        save_results({
            'use_case': '1. Ingestion (Vector Data)',
            'technology': 'GeoPandas',
            'operation_description': 'Read Shapefile into GeoDataFrame',
            'test_dataset': shapefile_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_ingestion_times),
            'output_size_mb': 'N/A',
            'notes': f'Found {num_features} features. Average of {len(hot_ingestion_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_ingestion_times)} runs.")

    print("\nRunning GeoPandas Filtering (in-memory).")
    # Load data once for filtering tests
    gdf = gpd.read_file(shapefile_path)

    # Cold start run
    with Timer() as t:
        filtered_gdf = gdf[gdf['COD_REG'] == 1]
    cold_start_time = t.interval
    num_filtered_features = len(filtered_gdf)
    print(f"Cold start completed in {cold_start_time:.4f}s. Filtered to {num_filtered_features} features.")

    # Save file to disk and measure its size
    output_filename = 'comuni_filtered_geopandas.geoparquet'
    output_path = PROCESSED_DATA_DIR / 'geopandas_generated' / output_filename
    filtered_gdf.to_parquet(output_path)
    output_size_bytes = output_path.stat().st_size
    output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

    # Save cold start result
    save_results({
        'use_case': '2. Filtering (Vector Data)',
        'technology': 'GeoPandas',
        'operation_description': 'Filter GeoDataFrame by attribute',
        'test_dataset': shapefile_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': output_size_mb,
        'notes': f'Filtered to {num_filtered_features} features. Cold start (first run).'
    })

    # Hot start runs
    hot_filtering_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            with Timer() as t:
                _ = gdf[gdf['COD_REG'] == 1]
            hot_filtering_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # Save hot start results
        avg_hot_time = sum(hot_filtering_times) / len(hot_filtering_times)
        save_results({
            'use_case': '2. Filtering (Vector Data)',
            'technology': 'GeoPandas',
            'operation_description': 'Filter GeoDataFrame by attribute',
            'test_dataset': shapefile_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_filtering_times),
            'output_size_mb': output_size_mb,
            'notes': f'Filtered to {num_filtered_features} features. Average of {len(hot_filtering_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_filtering_times)} runs.")

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100

    if not SHAPEFILE_INPUT.exists():
        print(f"ERROR: Shapefile not found at {SHAPEFILE_INPUT}. Aborting the tests.")
    else:
        # Run benchmarks for all technologies
        benchmark_duckdb_vector(SHAPEFILE_INPUT, NUMBER_OF_RUNS)
        benchmark_postgis_vector(NUMBER_OF_RUNS)
        benchmark_geopandas_vector(SHAPEFILE_INPUT, NUMBER_OF_RUNS)

        print("\nAll vector data benchmarks are complete.")
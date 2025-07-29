import rioxarray
import osmnx as ox
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
# Update this filename to match the GHS-POP file you downloaded
RASTER_INPUT = RAW_DATA_DIR / 'raster' / 'GHS_POP_ITALY_100m.tif'

# Ensure the output directory exists
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def run_duckdb_raster_benchmark(raster_path, place_name):
    """
    Adds placeholder results for DuckDB, as it does not support raster data.
    """
    print("\nTesting DuckDB Spatial ingestion & filtering for Raster Data.")
    print("SKIPPING: DuckDB Spatial currently does not support native raster operations.")

    # Save a placeholder result for Ingestion
    ingestion_result = {
        'use_case': '1. Ingestion (Raster Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Open GeoTIFF file',
        'test_dataset': raster_path.name,
        'execution_time_s': 'N/A',
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': 'Technology does not support native raster operations.'
    }
    save_results(ingestion_result)

    # Save a placeholder result for Filtering
    filtering_result = {
        'use_case': '2. Filtering (Raster Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': f'Clip raster to {place_name.split(",")[0]}',
        'test_dataset': raster_path.name,
        'execution_time_s': 'N/A',
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': 'Technology does not support native raster operations.'
    }
    save_results(filtering_result)

def run_postgis_raster_benchmark(place_name, num_runs=100):
    """
    Runs Filtering benchmarks for PostGIS on a raster table.
    NOTE: Ingestion must be performed and timed manually as a one-time setup cost,
    using raster2pgsql tool.
    """
    print("\nTesting PostGIS filtering for Raster Data.")

    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')
        place_name_clean = place_name.split(',')[0]
        boundary_gdf = ox.geocode_to_gdf(place_name)
        area_wkt = boundary_gdf.geometry.iloc[0].wkt

        query = f"""
        SELECT ST_AsGDALRaster(ST_Clip(rast, 
            ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast))
        ), 'GTiff') 
        FROM public.ghs_population
        WHERE ST_Intersects(rast, ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast)));
        """

        print(f"\nRunning PostGIS Filtering (clip to {place_name_clean}).")

        # Cold start run
        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            _ = cursor.fetchall()
            cursor.close()
        cold_start_time = t.interval
        print(f"Cold start completed in {cold_start_time:.4f}s.")

        pop_query = f"""
        WITH clipped_raster AS (
            SELECT ST_Clip(rast, 
                ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast))
            ) AS clipped_rast
            FROM public.ghs_population
            WHERE ST_Intersects(rast, ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast)))
        )
        SELECT (stats).sum
        FROM (SELECT ST_SummaryStats(ST_Union(clipped_rast)) AS stats FROM clipped_raster) AS summary;
        """
        cursor = conn.cursor()
        cursor.execute(pop_query)
        total_population = int(cursor.fetchone()[0])
        cursor.close()
        print(f"Calculated total population for {place_name_clean} (PostGIS): {total_population:,}")

        # Save cold start results
        save_results({
            'use_case': '2. Filtering (Raster Data)',
            'technology': 'PostGIS',
            'operation_description': f'Clip raster to {place_name_clean}',
            'test_dataset': 'ghs_population table',
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': 'N/A',
            'notes': f'Found {total_population:,} people. Cold start query time (first run).'
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
                'use_case': '2. Filtering (Raster Data)',
                'technology': 'PostGIS',
                'operation_description': f'Clip raster to {place_name_clean}',
                'test_dataset': 'ghs_population table',
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_filtering_times),
                'output_size_mb': 'N/A',
                'notes': f'Found {total_population:,} people. Average of {len(hot_filtering_times)} hot cache query times.'
            })
            print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_filtering_times)} runs.")

    except Exception as e:
        print(f"An error occurred during PostGIS test: {e}")
    finally:
        if conn:
            conn.close()

def run_python_raster_benchmark(raster_path, place_name, num_runs=100):
    """
    Runs Ingestion and Filtering benchmarks for Python (rioxarray) on a raster file.
    """
    print("\nTesting Python (rioxarray) ingestion & filtering for Raster Data.")

    print("\nRunning Python (rioxarray) Ingestion (rioxarray.open_rasterio).")

    # Cold start run
    with Timer() as t:
        rds = rioxarray.open_rasterio(raster_path)
        rds.close()  # Close the file handle
    cold_start_time = t.interval
    print(f"Cold start completed in {cold_start_time:.4f}s.")

    # Save cold start results
    save_results({
        'use_case': '1. Ingestion (Raster Data)',
        'technology': 'Python (rioxarray)',
        'operation_description': 'Open GeoTIFF file',
        'test_dataset': raster_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': 'Cold start (first run).'
    })

    # Hot start runs
    hot_ingestion_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            with Timer() as t:
                rds = rioxarray.open_rasterio(raster_path)
                rds.close()
            hot_ingestion_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # Save hot start results
        avg_hot_time = sum(hot_ingestion_times) / len(hot_ingestion_times)
        save_results({
            'use_case': '1. Ingestion (Raster Data)',
            'technology': 'Python (rioxarray)',
            'operation_description': 'Open GeoTIFF file',
            'test_dataset': raster_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_ingestion_times),
            'output_size_mb': 'N/A',
            'notes': f'Average of {len(hot_ingestion_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_ingestion_times)} runs.")

    print(f"\nRunning Python (rioxarray) Filtering (clip to {place_name.split(',')[0]}).")

    # Load data once for filtering tests
    rds = rioxarray.open_rasterio(raster_path)
    boundary_gdf = ox.geocode_to_gdf(place_name)

    # Cold start run
    with Timer() as t:
        clipped_rds = rds.rio.clip(boundary_gdf.geometry.to_list(), boundary_gdf.crs, drop=True)
    cold_start_time = t.interval
    print(f"Cold start completed in {cold_start_time:.4f}s.")

    total_population = int(clipped_rds.sum())
    print(f"Calculated total population for {place_name.split(',')[0]} (rioxarray): {total_population:,}")

    # Save clipped file to measure size
    output_path = PROCESSED_DATA_DIR / "geopandas_generated" / f"{place_name.split(',')[0].lower()}_pop.tif"
    clipped_rds.rio.to_raster(output_path, tiled=True, compress='LZW')
    output_size_bytes = output_path.stat().st_size
    output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

    # Save cold start results
    save_results({
        'use_case': '2. Filtering (Raster Data)',
        'technology': 'Python (rioxarray)',
        'operation_description': f'Clip raster to {place_name.split(",")[0]}',
        'test_dataset': raster_path.name,
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': output_size_mb,
        'notes': f'Found {total_population:,} people. Cold start (first run).'
    })

    # Hot start runs
    hot_filtering_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            with Timer() as t:
                _ = rds.rio.clip(boundary_gdf.geometry.to_list(), boundary_gdf.crs, drop=True)
            hot_filtering_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        # Save hot start results
        avg_hot_time = sum(hot_filtering_times) / len(hot_filtering_times)
        save_results({
            'use_case': '2. Filtering (Raster Data)',
            'technology': 'Python (rioxarray)',
            'operation_description': f'Clip raster to {place_name.split(",")[0]}',
            'test_dataset': raster_path.name,
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_filtering_times),
            'output_size_mb': output_size_mb,
            'notes': f'Found {total_population:,} people. Average of {len(hot_filtering_times)} hot cache runs.'
        })
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_filtering_times)} runs.")

    rds.close()

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100
    PLACE_TO_BENCHMARK = "Milan, Italy"

    if not RASTER_INPUT.exists():
        print(f"ERROR: Raster file not found at {RASTER_INPUT}.")
        print("Please download the GHS-POP data and place it in the correct directory.")
    else:
        # Run benchmarks for all technologies
        run_duckdb_raster_benchmark(RASTER_INPUT, PLACE_TO_BENCHMARK)
        run_postgis_raster_benchmark(PLACE_TO_BENCHMARK, NUMBER_OF_RUNS)
        run_python_raster_benchmark(RASTER_INPUT, PLACE_TO_BENCHMARK, NUMBER_OF_RUNS)

        print("\nAll raster data benchmarks are complete.")
import geopandas as gpd
import psycopg2
from rasterstats import zonal_stats
from pathlib import Path
import sys
import rioxarray

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
RAW_DATA_DIR = WORKING_ROOT / 'data' / 'raw'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'
RASTER_INPUT = RAW_DATA_DIR / 'raster' / 'GHS_POP_ITALY_100m.tif'
VECTOR_INPUT = RAW_DATA_DIR / 'comuni_istat' / 'Com01012025_WGS84.shp'

def run_postgis_vector_raster_analysis(num_runs=100):
    """
    Runs Vector-Raster Analysis benchmarks for PostGIS.
    """
    print(f"\nTesting PostGIS Vector-Raster Analysis.")

    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')

        # First, let's check what columns are actually available in the table
        check_columns_query = """
                              SELECT column_name
                              FROM information_schema.columns
                              WHERE table_schema = 'vector_data'
                                AND table_name = 'comuni_istat_clean'; \
                              """

        cursor = conn.cursor()
        cursor.execute(check_columns_query)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()

        # Find the correct column names for municipality name and region code
        # Common variations for Italian municipality data
        comune_col = None
        reg_col = None

        # Priority order for municipality name
        for col in ['COMUNE', 'COMUNE_A', 'NOME']:
            if col in columns:
                comune_col = col
                break

        # Priority order for region code
        for col in ['COD_REG', 'COD_REGIONE']:
            if col in columns:
                reg_col = col
                break

        if not comune_col or not reg_col:
            print(f"Could not find municipality or region columns. Available: {columns}")
            return

        print(f"Using columns municipality='{comune_col}' and region='{reg_col}'.")

        # This query calculates the sum of population for each municipality in Piedmont.
        # It joins the vector comuni_istat table with the raster ghs_population table.
        # Using ST_ValueCount (PVC - Pixel Value Count) instead of ST_SummaryStats to avoid -inf issues
        query = f"""
                SELECT
                    c."{comune_col}",
                    COALESCE(
                        SUM(
                            CASE 
                                WHEN (pvc).value > 0 AND (pvc).value < 1e10 
                                THEN (pvc).value * (pvc).count 
                                ELSE 0 
                            END
                        ), 0
                    ) AS total_population
                FROM 
                    vector_data.comuni_istat_clean c
                CROSS JOIN LATERAL (
                    SELECT ST_ValueCount(ST_Clip(p.rast, c.geometry, true)) as pvc
                    FROM raster_data.ghs_population p 
                    WHERE ST_Intersects(p.rast, c.geometry)
                ) AS subq
                WHERE
                    c."{reg_col}" = '{TARGET_REGION_CODE}'
                    AND (pvc).value IS NOT NULL
                GROUP BY
                    c."{comune_col}";
                """

        # Cold start run
        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
        cold_start_time = t.interval

        # Calculate total population
        total_population = sum(item[1] for item in results if item[1] is not None)
        print(f"Cold start completed in {cold_start_time:.4f}s. Calculated stats for {len(results)} municipalities.")
        print(f"Total population for Piedmont (PostGIS): {total_population:,.0f}")

        # Save cold start results
        save_results({
            'use_case': '5. Vector-Raster Analysis (Vector Data & Raster Data)',
            'technology': 'PostGIS',
            'operation_description': 'Calculate Population per Municipality',
            'test_dataset': 'comuni_istat_clean and ghs_population tables',
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': 'N/A',
            'notes': f'Total Population: {total_population:,.0f} in {len(results)} municipalities. Cold start (first run).'
        })

        # Hot start runs
        hot_start_times = []
        if num_runs > 1:
            for i in range(num_runs - 1):
                with Timer() as t:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    _ = cursor.fetchall()
                    cursor.close()
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            print("\n")

            avg_hot_time = sum(hot_start_times) / len(hot_start_times)
            print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_start_times)} runs.")

            save_results({
                'use_case': '5. Vector-Raster Analysis (Vector Data & Raster Data)',
                'technology': 'PostGIS',
                'operation_description': 'Calculate Population per Municipality',
                'test_dataset': 'comuni_istat_clean and ghs_population tables',
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': 'N/A',
                'notes': f'Total Population: {total_population:,.0f} in {len(results)} municipalities. Average of {len(hot_start_times)} hot cache runs.'
            })

    except Exception as e:
        print(f"An error occurred during PostGIS test: {e}.")
    finally:
        if conn:
            conn.close()

def run_python_vector_raster_analysis(vector_path, raster_path, num_runs=100):
    """
    Runs Vector-Raster Analysis benchmarks for the Python stack (GeoPandas + rasterstats).
    """
    print(f"\nTesting Python stack Vector-Raster Analysis.")

    # Load and prepare vector data
    comuni_gdf = gpd.read_file(vector_path)
    piedmont_gdf = comuni_gdf[comuni_gdf['COD_REG'] == TARGET_REGION_CODE].copy()

    # Reproject vector data to match the raster's CRS
    raster_crs = rioxarray.open_rasterio(raster_path).rio.crs
    piedmont_gdf = piedmont_gdf.to_crs(raster_crs)

    # Cold start run
    with Timer() as t:
        stats = zonal_stats(piedmont_gdf, str(raster_path), stats="sum", nodata=-200)
    cold_start_time = t.interval

    # Calculate total population
    total_population = sum(item['sum'] for item in stats if item['sum'] is not None)
    print(f"Cold start completed in {cold_start_time:.4f}s. Calculated stats for {len(stats)} municipalities.")
    print(f"Total population for Piemonte (Python): {total_population:,.0f}")

    # Save cold start results
    save_results({
        'use_case': '5. Vector-Raster Analysis (Vector Data & Raster Data)',
        'technology': 'Python (rasterstats)',
        'operation_description': 'Calculate Population per Municipality',
        'test_dataset': f'{vector_path.name} and {raster_path.name}',
        'execution_time_s': cold_start_time,
        'num_runs': 1,
        'output_size_mb': 'N/A',
        'notes': f'Total Population: {total_population:,.0f} in {len(stats)} municipalities. Cold start (first run).'
    })

    # Hot start runs
    hot_start_times = []
    if num_runs > 1:
        for i in range(num_runs - 1):
            with Timer() as t:
                _ = zonal_stats(piedmont_gdf, str(raster_path), stats="sum")
            hot_start_times.append(t.interval)
            print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
        print("\n")

        avg_hot_time = sum(hot_start_times) / len(hot_start_times)
        print(f"Average hot start: {avg_hot_time:.4f}s over {len(hot_start_times)} runs.")

        save_results({
            'use_case': '5. Vector-Raster Analysis (Vector Data & Raster Data)',
            'technology': 'Python (rasterstats)',
            'operation_description': 'Calculate Population per Municipality',
            'test_dataset': f'{vector_path.name} and {raster_path.name}',
            'execution_time_s': avg_hot_time,
            'num_runs': len(hot_start_times),
            'output_size_mb': 'N/A',
            'notes': f'Total Population: {total_population:,.0f} in {len(stats)} municipalities. Average of {len(hot_start_times)} hot cache runs.'
        })

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100
    TARGET_REGION_CODE = 1  # 1 = Piedmont

    if not RASTER_INPUT.exists() or not VECTOR_INPUT.exists():
        print(f"ERROR: Input file(s) not found. Please check the paths or the files themselves.")
    else:
        # Run benchmarks for both technologies
        run_postgis_vector_raster_analysis(NUMBER_OF_RUNS)
        run_python_vector_raster_analysis(VECTOR_INPUT, RASTER_INPUT, NUMBER_OF_RUNS)

        print("\nAll Vector Data and Raster Data tests for Use Case 5 are complete.")
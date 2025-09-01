import duckdb
from pathlib import Path
import sys
import geopandas as gpd

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

def run_duckdb_single_table_analysis(city_name, main_file_path, secondary_file_path=None, num_runs=100):
    """
    Runs selected Use Case 3 benchmarks for DuckDB on a given city's datasets.
    It can handle both single-file and two-file (join-like) operations.
    """
    metric_crs = 'EPSG:32632'  # WGS 84 / UTM zone 32N for metric calculations

    # List of the 3 selected operations for the benchmark.
    operations = [
        {
            'name': '3.1. Top 10 Largest Areas (sqm)',
            'query': f"SELECT ST_Area(ST_Transform(geometry, 'OGC:CRS84', '{metric_crs}')) AS area_sqm, ST_AsWKB(geometry) as geom_wkb FROM read_parquet('{{main_file}}') ORDER BY area_sqm DESC LIMIT 10",
            'requires_secondary_file': False
        },
        {
            'name': '3.2. Total Buffered Area (sqm)',
            'query': f"SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'OGC:CRS84', '{metric_crs}'), 10.0))) AS total_buffered_area FROM read_parquet('{{main_file}}')",
            'requires_secondary_file': False
        },
        {
            'name': '3.3. Restaurants NOT near Bus Stops',
            'query': f"""
                     SELECT r.feature_id, ST_AsWKB(r.geometry) as geom_wkb
                     FROM read_parquet('{{main_file}}') AS r
                     WHERE NOT EXISTS (SELECT 1
                                       FROM read_parquet('{{secondary_file}}') AS bs
                                       WHERE ST_DWithin(
                                           ST_Transform(r.geometry, 'OGC:CRS84', '{metric_crs}'), 
                                           ST_Transform(bs.geometry, 'OGC:CRS84', '{metric_crs}'), 
                                           50.0
                                       ))
                     """,
            'requires_secondary_file': True
        }
    ]

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    for op in operations:
        # Determine if this operation should be run based on the provided files
        if (op['requires_secondary_file'] and not secondary_file_path) or \
                (not op['requires_secondary_file'] and secondary_file_path):
            continue

        print(f"\nRunning Operation '{op['name']}' for {city_name.title()}.")

        dataset_name = f"{city_name.lower()}_restaurants_bus_stops.geoparquet" if op[
            'requires_secondary_file'] else main_file_path.name

        sql_query = op['query'].format(
            main_file=str(main_file_path).replace('\\', '/'),
            secondary_file=str(secondary_file_path).replace('\\', '/') if secondary_file_path else ''
        )

        # Cold start run
        with Timer() as t:
            result_df = con.execute(sql_query).df()
        cold_start_time = t.interval
        print(f"Cold start completed in {cold_start_time:.6f}s.")

        # Print a cleaner preview by dropping the WKB column
        print("Result Preview:")
        print(result_df.drop(columns=['geom_wkb'], errors='ignore').to_string())

        # Initialize output size as N/A
        output_size_mb = 'N/A'

        # Save GeoParquet output for operations with geometry
        if 'geom_wkb' in result_df.columns:
            # Convert bytearray to bytes before creating GeoSeries
            result_gdf = gpd.GeoDataFrame(
                result_df.drop(columns=['geom_wkb']),
                geometry=gpd.GeoSeries.from_wkb(result_df['geom_wkb'].apply(bytes)),
                crs="EPSG:4326"
            )

            op_filename_part = op['name'].split('.')[1].strip().lower().replace(' ', '_')
            output_filename = f"{city_name.lower()}_{op_filename_part}_duckdb.geoparquet"
            output_path = PROCESSED_DATA_DIR / 'duckdb_generated' / output_filename
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
            result_gdf.to_parquet(output_path)
            print(f"Output saved to {output_path.relative_to(WORKING_ROOT.parent)}.")

            # Calculate file size
            output_size_bytes = output_path.stat().st_size
            output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

        # Custom notes logic
        if op['name'] == '3.2. Total Buffered Area (sqm)':
            total_area = result_df.iloc[0, 0]
            cold_notes = f'Total area: {total_area:,.2f} sqm. Cold start (first run).'
            hot_notes_template= f'Total area: {total_area:,.2f} sqm. Average of {{count}} hot cache runs.'
        else:
            cold_notes = f'Found {len(result_df)} results. Cold start (first run).'
            hot_notes_template = f'Found {len(result_df)} results. Average of {{count}} hot cache runs.'

        # Save cold run results
        save_results({
            'use_case': '3. Single Table Analysis (OSM Data)',
            'technology': 'DuckDB Spatial',
            'operation_description': op['name'],
            'test_dataset': dataset_name,
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': output_size_mb,
            'notes': cold_notes
        })

        # Hot start runs
        hot_start_times = []
        if num_runs > 1:
            for i in range(num_runs - 1):
                with Timer() as t:
                    _ = con.execute(sql_query).df()
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.6f}s.", end='\r')
            print("\n")

            avg_hot_time = sum(hot_start_times) / len(hot_start_times)
            print(f"Average hot start: {avg_hot_time:.6f}s over {len(hot_start_times)} runs.")
            hot_notes = hot_notes_template.format(count=len(hot_start_times))

            # Save hot run results
            save_results({
                'use_case': '3. Single Table Analysis (OSM Data)',
                'technology': 'DuckDB Spatial',
                'operation_description': op['name'],
                'test_dataset': dataset_name,
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': output_size_mb,
                'notes': hot_notes
            })

    con.close()

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100

    # Define all the datasets needed for the benchmarks
    datasets_by_city = {
        'Pinerolo': {
            'buildings': PROCESSED_DATA_DIR / 'pinerolo_buildings.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'pinerolo_restaurants.geoparquet',
            'bus_stops': PROCESSED_DATA_DIR / 'pinerolo_bus_stops.geoparquet'
        },
        'Milan': {
            'buildings': PROCESSED_DATA_DIR / 'milan_buildings.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'milan_restaurants.geoparquet',
            'bus_stops': PROCESSED_DATA_DIR / 'milan_bus_stops.geoparquet'
        },
        'Rome': {
            'buildings': PROCESSED_DATA_DIR / 'rome_buildings.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'rome_restaurants.geoparquet',
            'bus_stops': PROCESSED_DATA_DIR / 'rome_bus_stops.geoparquet'
        }
    }

    # Loop through each city and run the appropriate benchmarks
    for city, paths in datasets_by_city.items():
        # Print header only once per city
        print(f"\nTesting DuckDB Single Table Analysis Operations for: {city.upper()}.")

        # Run single-table benchmarks using the 'buildings' file
        if paths['buildings'].exists():
            run_duckdb_single_table_analysis(
                city_name=city, main_file_path=paths['buildings'], num_runs=NUMBER_OF_RUNS
            )
        else:
            print(f"\nERROR: Buildings file for {city} not found. Skipping single-table tests.")

        # Run two-table benchmark using 'restaurants' and 'bus_stops' files
        if paths['restaurants'].exists() and paths['bus_stops'].exists():
            run_duckdb_single_table_analysis(
                city_name=city, main_file_path=paths['restaurants'],
                secondary_file_path=paths['bus_stops'], num_runs=NUMBER_OF_RUNS
            )
        else:
            print(f"\nERROR: Restaurants or bus stops file for {city} not found. Skipping join-like test.")

    print("\nAll DuckDB tests for Use Case 3 are complete.")
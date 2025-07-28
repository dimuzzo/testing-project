import psycopg2
from pathlib import Path
import sys

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

def run_postgis_single_table_analysis(city_name, buildings_table, restaurants_table, bus_stops_table, num_runs=100):
    """
    Runs selected Use Case 3 benchmarks for PostGIS on a given city's datasets.
    """
    metric_srid = 32632  # WGS 84 / UTM zone 32N for metric calculations

    # List of the 3 selected operations for the benchmark.
    operations = [
        {
            'name': '3.1. Top 10 Largest Areas (sqm)',
            'query': f"SELECT ST_Area(ST_Transform(geom, {metric_srid})) AS area_sqm FROM {buildings_table} ORDER BY area_sqm DESC LIMIT 10",
            'type': 'single_table'
        },
        {
            'name': '3.2. Total Buffered Area (sqm)',
            'query': f"SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geom, {metric_srid}), 10.0))) AS total_buffered_area FROM {buildings_table}",
            'type': 'single_table'
        },
        {
            'name': '3.3. Restaurants NOT near Bus Stops',
            'query': f"""
                     SELECT r.feature_id
                     FROM {restaurants_table} AS r
                     WHERE NOT EXISTS (SELECT 1
                                       FROM {bus_stops_table} AS bs
                                       WHERE ST_DWithin(
                                           ST_Transform(r.geom, {metric_srid}),
                                           ST_Transform(bs.geom, {metric_srid}),
                                           50.0
                                       ))
                     """,
            'type': 'two_tables'
        }
    ]

    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')

        for op in operations:
            print(f"\nRunning Operation: '{op['name']}'.")

            dataset_name = f"{city_name.lower()}_tables"

            # Cold start run
            with Timer() as t:
                cursor = conn.cursor()
                cursor.execute(op['query'])
                result = cursor.fetchall()
                cursor.close()
            cold_start_time = t.interval
            print(f"Cold start completed in {cold_start_time:.6f}s.")

            # Print a cleaner preview by dropping the WKB column
            print("Result Preview:")
            print(result_df.drop(columns=['geom_wkb'], errors='ignore').to_string())

            save_results({
                'use_case': '3. Single Table Analysis',
                'technology': 'PostGIS',
                'operation_description': op['name'],
                'test_dataset': dataset_name,
                'execution_time_s': cold_start_time,
                'num_runs': 1,
                'output_size_mb': 'N/A',
                'notes': f'Found {len(result)} results. Cold start (first run).'
            })

            # Hot start runs
            hot_start_times = []
            if num_runs > 1:
                for i in range(num_runs - 1):
                    with Timer() as t:
                        cursor = conn.cursor()
                        cursor.execute(op['query'])
                        _ = cursor.fetchall()
                        cursor.close()
                    hot_start_times.append(t.interval)
                    print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.6f}s.", end='\r')
                print()

                avg_hot_time = sum(hot_start_times) / len(hot_start_times)
                print(f"Average hot start: {avg_hot_time:.6f}s over {len(hot_start_times)} runs.")

                save_results({
                    'use_case': '3. Single Table Analysis',
                    'technology': 'PostGIS',
                    'operation_description': op['name'],
                    'test_dataset': dataset_name,
                    'execution_time_s': avg_hot_time,
                    'num_runs': len(hot_start_times),
                    'output_size_mb': 'N/A',
                    'notes': f'Found {len(result)} results. Average of {len(hot_start_times)} hot cache runs.'
                })

    except Exception as e:
        print(f"An error occurred during PostGIS benchmark: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100

    # Define all the datasets' tables needed for the benchmarks
    datasets_by_city = {
        'Pinerolo': {
            'buildings': 'pinerolo_buildings',
            'restaurants': 'pinerolo_restaurants',
            'bus_stops': 'pinerolo_bus_stops'
        },
        'Milan': {
            'buildings': 'milan_buildings',
            'restaurants': 'milan_restaurants',
            'bus_stops': 'milan_bus_stops'
        },
        'Rome': {
            'buildings': 'rome_buildings',
            'restaurants': 'rome_restaurants',
            'bus_stops': 'rome_bus_stops'
        }
    }

    for city, tables in datasets_by_city.items():
        # Print header only once per city
        print(f"\nTesting PostGIS Single Table Analysis Operations for: {city.upper()}.")

        run_postgis_single_table_analysis(
            city_name=city,
            buildings_table=tables['buildings'],
            restaurants_table=tables['restaurants'],
            bus_stops_table=tables['bus_stops'],
            num_runs=NUMBER_OF_RUNS
        )

    print("\nAll PostGIS tests for Use Case 3 are complete.")
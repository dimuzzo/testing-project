import geopandas as gpd
from pathlib import Path
import sys
import pandas as pd

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

def run_geopandas_single_table_analysis(city_name, buildings_path, restaurants_path, bus_stops_path, num_runs=100):
    """
    Runs selected Use Case 3 benchmarks for GeoPandas on a given city's datasets.
    """
    metric_crs = "EPSG:32632" # WGS 84 / UTM zone 32N for metric calculations

    # Load all necessary dataframes once
    gdf_buildings = gpd.read_parquet(buildings_path)
    gdf_restaurants = gpd.read_parquet(restaurants_path)
    gdf_bus_stops = gpd.read_parquet(bus_stops_path)

    # Define each operation as a separate function
    def op_top_10_areas(gdf):
        gdf_valid = gdf.copy()
        gdf_valid.geometry = gdf_valid.geometry.buffer(0)
        gdf_metric = gdf_valid.to_crs(metric_crs)
        gdf_metric['area_sqm'] = gdf_metric.geometry.area
        return gdf_metric.sort_values(by='area_sqm', ascending=False).head(10)

    def op_total_buffered_area(gdf):
        gdf_valid = gdf.copy()
        gdf_valid.geometry = gdf_valid.geometry.buffer(0)
        gdf_metric = gdf_valid.to_crs(metric_crs)
        total_area = gdf_metric.geometry.buffer(10).area.sum()
        return pd.DataFrame([{'total_buffered_area': total_area}])

    def op_restaurants_not_near_bus_stops(gdf_rest, gdf_bus):
        rest_valid = gdf_rest.copy()
        rest_valid.geometry = rest_valid.geometry.buffer(0)
        rest_metric = rest_valid.to_crs(metric_crs)

        bus_valid = gdf_bus.copy()
        bus_valid.geometry = bus_valid.geometry.buffer(0)
        bus_metric = bus_valid.to_crs(metric_crs)

        # Find indices of restaurants that ARE within 50 meters of a bus stop
        intersecting_indices = gpd.sjoin_nearest(
            rest_metric, bus_metric, how='inner', max_distance=50
        ).index

        # Return restaurants whose indices are NOT in the intersecting list
        return rest_metric[~rest_metric.index.isin(intersecting_indices)]

    # Define the list of operations to run
    operations = [
        {'name': '3.1. Top 10 Largest Areas (sqm)',
         'func': op_top_10_areas,
         'data': [gdf_buildings],
         'dataset_name': buildings_path.name
         },
        {'name': '3.2. Total Buffered Area (sqm)',
         'func': op_total_buffered_area,
         'data': [gdf_buildings],
         'dataset_name': buildings_path.name
         },
        {'name': '3.3. Restaurants NOT near Bus Stops',
         'func': op_restaurants_not_near_bus_stops,
         'data': [gdf_restaurants, gdf_bus_stops],
         'dataset_name': f"{city_name.lower()}_restaurants_bus_stops"
         }
    ]

    for op in operations:
        print(f"\nRunning Operation '{op['name']}'.")

        # Cold start run
        with Timer() as t:
            result_df = op['func'](*op['data'])
        cold_start_time = t.interval
        print(f"Cold start completed in {cold_start_time:.6f}s.")

        print("Result Preview:")
        if op['name'] == '3.2. Total Buffered Area (sqm)':
            total_area = result_df.iloc[0, 0]
            print(f"Total buffered area: {total_area:,.2f} sqm")
        else:
            # Drop the long geometry column for a cleaner print
            print(result_df.drop(columns=['geometry'], errors='ignore').to_string())

        output_size_mb = 'N/A'

        if isinstance(result_df, gpd.GeoDataFrame):
            op_filename_part = op['name'].split('.')[1].strip().lower().replace(' ', '_')
            output_filename = f"{city_name.lower()}_{op_filename_part}_geopandas.geoparquet"
            output_path = PROCESSED_DATA_DIR / 'geopandas_generated' /output_filename
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
            result_df.to_parquet(output_path)
            print(f"Output saved to {output_path.relative_to(WORKING_ROOT.parent)}.")

            # Calculate file size
            output_size_bytes = output_path.stat().st_size
            output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

        # Custom notes logic
        if op['name'] == '3.2. Total Buffered Area (sqm)':
            total_area = result_df.iloc[0, 0]
            cold_notes = f'Total area: {total_area:,.2f} sqm. Cold start (first run).'
            hot_notes_template = f'Total area: {total_area:,.2f} sqm. Average of {{count}} hot cache runs.'
        else:
            cold_notes = f'Found {len(result_df)} results. Cold start (first run).'
            hot_notes_template = f'Found {len(result_df)} results. Average of {{count}} hot cache runs.'

        # Save cold run results
        save_results({
            'use_case': '3. Single Table Analysis (OSM Data)',
            'technology': 'GeoPandas',
            'operation_description': op['name'],
            'test_dataset': op['dataset_name'],
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
                    _ = op['func'](*op['data'])
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.6f}s.", end='\r')
            print()

            avg_hot_time = sum(hot_start_times) / len(hot_start_times)
            print(f"Average hot start: {avg_hot_time:.6f}s over {len(hot_start_times)} runs.")
            hot_notes = hot_notes_template.format(count=len(hot_start_times))

            # Save hot run results
            save_results({
                'use_case': '3. Single Table Analysis (OSM Data)',
                'technology': 'GeoPandas',
                'operation_description': op['name'],
                'test_dataset': op['dataset_name'],
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': output_size_mb,
                'notes': hot_notes
            })

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

    for city, paths in datasets_by_city.items():
        if all(p.exists() for p in paths.values()):
            # Print header only once per city
            print(f"\nTesting GeoPandas Single Table Analysis Operations for: {city.upper()}.")

            run_geopandas_single_table_analysis(
                city_name=city,
                buildings_path=paths['buildings'],
                restaurants_path=paths['restaurants'],
                bus_stops_path=paths['bus_stops'],
                num_runs=NUMBER_OF_RUNS
            )
        else:
            print(f"\nERROR: One or more data files for {city} not found. Skipping its tests.")

    print("\nAll GeoPandas tests for Use Case 3 are complete.")
import geopandas as gpd
from pathlib import Path
import sys
import osmnx as ox
import pandas as pd

# Add the parent directory of 'scripts' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

def run_geopandas_complex_spatial_join(city_name, num_runs=100, **file_paths):
    """
    Runs selected Use Case 4 benchmarks for GeoPandas on a given city's datasets.
    Handles operations with a variable number of input files via **file_paths.
    """
    metric_crs = 'EPSG:32632' # WGS 84 / UTM zone 32N for metric calculations

    # Load all dataframes once at the beginning
    gdfs = {}
    for key, path in file_paths.items():
        # Check for Path objects and existence, ignoring the city_boundary_gdf which is not a path
        if isinstance(path, Path) and path.exists():
            gdfs[key.replace('_file', '')] = gpd.read_parquet(path)

    city_boundary_gdf = file_paths.get('city_boundary_gdf')

    # Functions of the 3 operations for the benchmark.
    def op_4_1_restaurants_per_neighborhood():
        if 'neighborhoods' not in gdfs or 'restaurants' not in gdfs: return None

        gdf_neighborhoods = gdfs['neighborhoods']
        gdf_restaurants = gdfs['restaurants']

        # Ensure we only work with polygons for an accurate point-in-polygon test
        polygons = gdf_neighborhoods[gdf_neighborhoods.geometry.type.isin(['Polygon', 'MultiPolygon'])].copy()

        # Perform the spatial join
        joined = gpd.sjoin(gdf_restaurants, polygons, how="right", predicate="within")

        # Group by the neighborhood index and count the restaurants
        restaurant_counts = joined.groupby(joined.index_right).size()

        # Create the final result dataframe
        result_gdf = polygons.join(restaurant_counts.rename('restaurant_count')).fillna(0)
        result_gdf['restaurant_count'] = result_gdf['restaurant_count'].astype(int)

        return result_gdf

    def op_4_2_tree_count_and_length_sum_of_residential_roads_near_hospitals():
        if 'hospitals' not in gdfs or 'residential_streets' not in gdfs or 'trees' not in gdfs: return None

        gdf_hospitals = gdfs['hospitals']
        gdf_streets = gdfs['residential_streets']
        gdf_trees = gdfs['trees']

        # Reproject all to metric CRS for distance calculations
        hospitals_metric = gdf_hospitals.to_crs(metric_crs)
        streets_metric = gdf_streets.to_crs(metric_crs)
        trees_metric = gdf_trees.to_crs(metric_crs)

        # Find streets within 100 meters of hospitals
        streets_near_hospitals_join = gpd.sjoin_nearest(streets_metric, hospitals_metric, max_distance=100, how='inner')
        unique_street_indices = streets_near_hospitals_join.index.unique()
        streets_near_hospitals = streets_metric.loc[unique_street_indices]

        # Find trees within 20 meters of that subset of streets
        trees_near_streets_join = gpd.sjoin_nearest(trees_metric, streets_near_hospitals, max_distance=20, how='inner')

        total_tree_count = trees_near_streets_join.index.nunique()
        total_street_length_m = streets_near_hospitals.geometry.length.sum()

        return pd.DataFrame({'total_tree_count': [total_tree_count], 'total_street_length_m': [total_street_length_m]})

    def op_4_3_area_not_covered_by_parks():
        if 'parks' not in gdfs or city_boundary_gdf is None: return None

        gdf_parks = gdfs['parks']

        # Reproject to metric CRS
        parks_metric = gdf_parks.to_crs(metric_crs)
        boundary_metric = city_boundary_gdf.to_crs(metric_crs)

        # Union all park geometries
        total_parks_geom = parks_metric.union_all()

        # Calculate the difference
        non_park_area_geom = boundary_metric.difference(total_parks_geom)

        # Calculate the final area
        non_park_area_sqm = non_park_area_geom.area.iloc[0]

        return pd.DataFrame({'non_park_area_sqm': [non_park_area_sqm]})

    # List of operations to run
    operations = [
        {'id': '4.1',
         'name': '4.1. Restaurants per Neighborhood',
         'func': op_4_1_restaurants_per_neighborhood,
         'required_files': ['neighborhoods_file', 'restaurants_file']
         },
        {'id': '4.2',
         'name': '4.2. Tree Count and Length Sum of Residential Roads Near Hospitals',
         'func': op_4_2_tree_count_and_length_sum_of_residential_roads_near_hospitals,
         'required_files': ['hospitals_file', 'residential_streets_file', 'trees_file']
         },
        {'id': '4.3',
         'name': '4.3. Area Not Covered by Parks',
         'func': op_4_3_area_not_covered_by_parks,
         'required_files': ['parks_file', 'city_boundary_gdf']
         }
    ]

    for op in operations:
        # Check if the operation can be run
        required = op['required_files']
        if not all(key in file_paths for key in required):
            continue

        print(f"\nRunning Operation '{op['name']}' for {city_name.title()}.")

        # Dynamically create the dataset name for logging
        dataset_name = f"{city_name.lower()}_{'_'.join(f.replace('_file', '').replace('_gdf', '') for f in required)}"

        # Cold start run
        with Timer() as t:
            result_df = op['func']()
        cold_start_time = t.interval
        print(f"Cold start completed in {cold_start_time:.6f}s.")

        # Print a preview
        print("Result Preview:")
        print(result_df.head().to_string())

        # Initialize output size as N/A
        output_size_mb = 'N/A'

        # Save GeoParquet output for operations with geometry
        if isinstance(result_df, gpd.GeoDataFrame):
            output_dir = PROCESSED_DATA_DIR / 'geopandas_generated'
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{city_name.lower()}_op_{op['id']}_geopandas.geoparquet"
            result_df.to_parquet(output_path)
            print(f"Output saved to {output_path.relative_to(WORKING_ROOT.parent)}.")

            # Calculate file size
            output_size_bytes = output_path.stat().st_size
            output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

        # Custom notes logic
        if op['id'] == '4.2':
            total_trees = result_df.iloc[0, 0]
            total_length = result_df.iloc[0, 1]
            cold_notes = f'Trees: {total_trees}, Street Length: {total_length or 0:,.2f}m. Cold start.'
            hot_notes_template = f'Trees: {total_trees}, Street Length: {total_length or 0:,.2f}m. Avg of {{count}} hot runs.'
        elif op['id'] == '4.3':
            total_area = result_df.iloc[0, 0]
            cold_notes = f'Non-park area: {total_area or 0:,.2f} sqm. Cold start.'
            hot_notes_template = f'Non-park area: {total_area or 0:,.2f} sqm. Avg of {{count}} hot runs.'
        else:
            cold_notes = f'Found {len(result_df)} results. Cold start (first run).'
            hot_notes_template = f'Found {len(result_df)} results. Average of {{count}} hot cache runs.'

        # Save cold run results
        save_results({
            'use_case': '4. Complex Spatial Join (OSM Data)',
            'technology': 'GeoPandas',
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
                    _ = op['func']()
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.6f}s.", end='\r')
            print("\n")

            avg_hot_time = sum(hot_start_times) / len(hot_start_times)
            print(f"Average hot start: {avg_hot_time:.6f}s over {len(hot_start_times)} runs.")
            hot_notes = hot_notes_template.format(count=len(hot_start_times))

            # Save hot run results
            save_results({
                'use_case': '4. Complex Spatial Join (OSM Data)',
                'technology': 'GeoPandas',
                'operation_description': op['name'],
                'test_dataset': dataset_name,
                'execution_time_s': avg_hot_time,
                'num_runs': len(hot_start_times),
                'output_size_mb': output_size_mb,
                'notes': hot_notes
            })

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100

    datasets_by_city = {
        'Pinerolo': {
            'neighborhoods': PROCESSED_DATA_DIR / 'pinerolo_neighborhoods.geoparquet',
            'parks': PROCESSED_DATA_DIR / 'pinerolo_parks.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'pinerolo_restaurants.geoparquet',
            'hospitals': PROCESSED_DATA_DIR / 'pinerolo_hospitals.geoparquet',
            'residential_streets': PROCESSED_DATA_DIR / 'pinerolo_residential_streets.geoparquet',
            'trees': PROCESSED_DATA_DIR / 'pinerolo_trees.geoparquet'
        },
        'Milan': {
            'neighborhoods': PROCESSED_DATA_DIR / 'milan_neighborhoods.geoparquet',
            'parks': PROCESSED_DATA_DIR / 'milan_parks.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'milan_restaurants.geoparquet',
            'hospitals': PROCESSED_DATA_DIR / 'milan_hospitals.geoparquet',
            'residential_streets': PROCESSED_DATA_DIR / 'milan_residential_streets.geoparquet',
            'trees': PROCESSED_DATA_DIR / 'milan_trees.geoparquet'
        },
        'Rome': {
            'neighborhoods': PROCESSED_DATA_DIR / 'rome_neighborhoods.geoparquet',
            'parks': PROCESSED_DATA_DIR / 'rome_parks.geoparquet',
            'restaurants': PROCESSED_DATA_DIR / 'rome_restaurants.geoparquet',
            'hospitals': PROCESSED_DATA_DIR / 'rome_hospitals.geoparquet',
            'residential_streets': PROCESSED_DATA_DIR / 'rome_residential_streets.geoparquet',
            'trees': PROCESSED_DATA_DIR / 'rome_trees.geoparquet'
        }
    }

    # Loop through each city and run the appropriate benchmarks
    for city, paths in datasets_by_city.items():
        print(f"Fetching authoritative boundary for {city}.")
        try:
            city_boundary_gdf = ox.geocode_to_gdf(f"{city}, Italy")
            city_boundary_gdf = city_boundary_gdf.to_crs("EPSG:4326")
            print("Boundary fetched successfully.")
        except Exception as e:
            print(f"Could not fetch boundary for {city}. Skipping tests that require it. Error: {e}")
            city_boundary_gdf = None

        # Add the fetched boundary to the dictionary to be passed to the worker function
        paths['city_boundary_gdf'] = city_boundary_gdf

        # Print header only once per city
        print(f"\nTesting GeoPandas Complex Spatial Join Operations for: {city.upper()}.")

        # Run all the Operations at once
        run_geopandas_complex_spatial_join(
            city_name=city,
            num_runs=NUMBER_OF_RUNS,
            **paths
        )

    print("\nAll GeoPandas tests for Use Case 4 are complete.")
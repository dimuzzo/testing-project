import geopandas as gpd
from pathlib import Path
import sys
import psycopg2
import osmnx as ox

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

def run_postgis_complex_spatial_join(city_name, num_runs=100, **kwargs):
    """
    Runs selected Use Case 4 benchmarks for PostGIS on a given city's datasets.
    """
    metric_srid = 32632  # WGS 84 / UTM zone 32N for metric calculations

    # List of the 3 selected operations for the benchmark.
    operations = [
        {
            'id': '4.1',
            'name': '4.1. Restaurants per Neighborhood',
            'required_tables': ['neighborhoods_table', 'restaurants_table'],
            'query': """
                     SELECT n.feature_id AS neighborhood_id, COUNT(r.feature_id) AS restaurant_count, n.geom
                     FROM "{neighborhoods_table}" AS n
                              LEFT JOIN "{restaurants_table}" AS r ON ST_Within(r.geom, n.geom)
                     WHERE ST_GeometryType(n.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
                     GROUP BY n.feature_id, n.geom;
                     """
        },
        {
            'id': '4.2',
            'name': '4.2. Tree Count and Length Sum of Residential Roads Near Hospitals',
            'required_tables': ['hospitals_table', 'residential_streets_table', 'trees_table'],
            'query': """
                     WITH streets_near_hospitals AS (SELECT DISTINCT s.feature_id, s.geom
                                                     FROM "{residential_streets_table}" AS s,
                                                          "{hospitals_table}" AS h
                                                     WHERE ST_DWithin(
                                                                   ST_Transform(s.geom, 4326, {metric_crs}),
                                                                   ST_Transform(h.geom, 4326, {metric_crs}),
                                                                   100.0)),
                          trees_near_streets AS (SELECT DISTINCT t.feature_id
                                                 FROM "{trees_table}" AS t,
                                                      streets_near_hospitals AS snh
                                                 WHERE ST_DWithin(
                                                               ST_Transform(t.geom, 4326, {metric_crs}),
                                                               ST_Transform(snh.geom, 4326, {metric_crs}),
                                                               20.0))
                     SELECT (SELECT COUNT(*) FROM trees_near_streets) AS total_tree_count,
                            (SELECT SUM(ST_Length(ST_Transform(geom, 4326, {metric_crs})))
                             FROM streets_near_hospitals)             AS total_street_length_m;
                     """
        },
        {
            'id': '4.3',
            'name': '4.3. Area Not Covered by Parks',
            'required_tables': ['parks_table', 'city_boundary_wkt'],
            'query': """
                     WITH parks_area AS (SELECT ST_Union(geom) AS geom
                                         FROM "{parks_table}"
                                         WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon'))
                     SELECT ST_Area(
                                    ST_Difference(
                                            ST_Transform(ST_SetSRID(ST_GeomFromText('{city_boundary_wkt}'), 4326), {metric_crs}),
                                            ST_Transform((SELECT geom FROM parks_area), 4326, {metric_crs})
                                    )
                            ) AS non_park_area_sqm;
                     """
        }
    ]

    conn = None
    try:
        conn = psycopg2.connect(dbname='osm_benchmark_db', user='postgres', password='postgres', host='localhost', port='5432')

        for op in operations:
            required = op['required_tables']
            if not all(key in kwargs for key in required):
                continue

            print(f"\nRunning Operation '{op['name']}' for {city_name.title()}.")

            dataset_name = f"{city_name.lower()}_{'_'.join(f.replace('_table', '').replace('_wkt', '') for f in required)}"

            # Format the query with table names and other parameters
            # NOTE: Not using f-strings for table names to prevent SQL injection risks,
            # even though in this controlled environment it's safe. It's good practice.
            query_params = {key: val for key, val in kwargs.items()}
            query_params['metric_crs'] = metric_srid
            sql_query = op['query'].format(**query_params)

            # Cold run
            with Timer() as t:
                result_df = gpd.read_postgis(sql_query, conn, geom_col='geom' if 'geom' in sql_query else None)
            cold_start_time = t.interval
            print(f"Cold start completed in {cold_start_time:.6f}s.")

            print("Result Preview:")
            print(result_df.drop(columns=['geom'], errors='ignore').head().to_string())

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
                'technology': 'PostGIS',
                'operation_description': op['name'],
                'test_dataset': dataset_name,
                'execution_time_s': cold_start_time,
                'num_runs': 1,
                'output_size_mb': 'N/A',
                'notes': cold_notes
            })

            # Hot start runs
            hot_start_times = []
            if num_runs > 1:
                for i in range(num_runs - 1):
                    with Timer() as t:
                        _ = gpd.read_postgis(sql_query, conn, geom_col='geom' if 'geom' in sql_query else None)
                    hot_start_times.append(t.interval)
                    print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.6f}s.", end='\r')
                print("\n")

                avg_hot_time = sum(hot_start_times) / len(hot_start_times)
                print(f"Average hot start: {avg_hot_time:.6f}s over {len(hot_start_times)} runs.")
                hot_notes = hot_notes_template.format(count=len(hot_start_times))

                # Save hot run results
                save_results({
                    'use_case': '4. Complex Spatial Join (OSM Data)',
                    'technology': 'PostGIS',
                    'operation_description': op['name'],
                    'test_dataset': dataset_name,
                    'execution_time_s': avg_hot_time,
                    'num_runs': len(hot_start_times),
                    'output_size_mb': 'N/A',
                    'notes': hot_notes
                })

    except Exception as e:
        print(f"An error occurred during PostGIS benchmark: {e}.")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    NUMBER_OF_RUNS = 100

    # Define all the datasets' tables needed for the benchmarks
    datasets_by_city = {
        'Pinerolo': {
            'neighborhoods': 'pinerolo_neighborhoods',
            'restaurants': 'pinerolo_restaurants',
            'hospitals': 'pinerolo_hospitals',
            'residential_streets': 'pinerolo_residential_streets',
            'trees': 'pinerolo_trees',
            'parks': 'pinerolo_parks'
        },
        'Milan': {
            'neighborhoods': 'milan_neighborhoods',
            'restaurants': 'milan_restaurants',
            'hospitals': 'milan_hospitals',
            'residential_streets': 'milan_residential_streets',
            'trees': 'milan_trees',
            'parks': 'milan_parks'
        },
        'Rome': {
            'neighborhoods': 'rome_neighborhoods',
            'restaurants': 'rome_restaurants',
            'hospitals': 'rome_hospitals',
            'residential_streets': 'rome_residential_streets',
            'trees': 'rome_trees',
            'parks': 'rome_parks'
        }
    }

    # Loop through each city and run the appropriate benchmarks
    for city, tables in datasets_by_city.items():
        print(f"Fetching authoritative boundary for {city}.")
        try:
            city_boundary_gdf = ox.geocode_to_gdf(f"{city}, Italy")
            city_boundary_gdf = city_boundary_gdf.to_crs("EPSG:4326")
            city_boundary_wkt = city_boundary_gdf.geometry.iloc[0].wkt
            print("Boundary fetched successfully.")
        except Exception as e:
            print(f"Could not fetch boundary for {city}. Skipping tests that require it. Error: {e}.")
            city_boundary_wkt = None

        # Print header only once per city
        print(f"\nTesting PostGIS Complex Spatial Join Operations for: {city.upper()}.")

        # Run all the Operations at once
        run_postgis_complex_spatial_join(
            city_name=city,
            num_runs=NUMBER_OF_RUNS,
            **tables,
            city_boundary_wkt=city_boundary_wkt
        )

    print("\nAll PostGIS tests for Use Case 4 are complete.")
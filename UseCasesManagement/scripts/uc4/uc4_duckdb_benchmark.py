import duckdb
from pathlib import Path
import sys
import geopandas as gpd
import osmnx as ox

# Add the parent directory of 'scripts' to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

def run_duckdb_complex_spatial_join(city_name, num_runs=100, **file_paths):
    """
    Runs selected Use Case 4 benchmarks for DuckDB on a given city's datasets.
    Handles operations with a variable number of input files via **file_paths.
    """
    metric_crs = 'EPSG:32632' # WGS 84 / UTM zone 32N for metric calculations

    # List of the 3 operations for the benchmark.
    operations = [
        {
            'id': '4.1',
            'name': '4.1. Restaurants per Neighborhood',
            'required_files': ['neighborhoods_file', 'restaurants_file'],
            'query': """
                     SELECT n.feature_id         AS neighborhood_id,
                            COUNT(r.feature_id)  AS restaurant_count,
                            ST_AsWKB(n.geometry) AS geom_wkb
                     FROM (SELECT *
                           FROM read_parquet('{neighborhoods_file}')
                           WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON')) AS n
                              LEFT JOIN read_parquet('{restaurants_file}') AS r ON ST_Within(r.geometry, n.geometry)
                     GROUP BY n.feature_id, n.geometry;
                     """
        },
        {
            'id': '4.2',
            'name': '4.2. Tree Count and Length Sum of Residential Roads Near Hospitals',
            'required_files': ['hospitals_file', 'residential_streets_file', 'trees_file'],
            'query': """
                     WITH streets_near_hospitals AS (SELECT DISTINCT s.feature_id, s.geometry
                                                     FROM read_parquet('{residential_streets_file}') AS s,
                                                          read_parquet('{hospitals_file}') AS h
                                                     WHERE ST_DWithin(
                                                                   ST_Transform(s.geometry, 'EPSG:4326', '{metric_crs}'),
                                                                   ST_Transform(h.geometry, 'EPSG:4326', '{metric_crs}'),
                                                                   100.0)),
                          trees_near_streets AS (SELECT DISTINCT t.feature_id
                                                 FROM read_parquet('{trees_file}') AS t,
                                                      streets_near_hospitals AS snh
                                                 WHERE ST_DWithin(ST_Transform(t.geometry, 'EPSG:4326', '{metric_crs}'),
                                                                  ST_Transform(snh.geometry, 'EPSG:4326', '{metric_crs}'),
                                                                  20.0))
                     SELECT (SELECT COUNT(*) FROM trees_near_streets) AS total_tree_count,
                            (SELECT SUM(ST_Length(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}')))
                             FROM streets_near_hospitals)             AS total_street_length_m;
                     """
        },
        {
            'id': '4.3',
            'name': '4.3. Area Not Covered by Parks',
            'required_files': ['parks_file', 'city_boundary_wkt'],
            'query': """
                     WITH parks_area AS (SELECT ST_Union_Agg(geometry) AS geom
                                         FROM read_parquet('{parks_file}')
                                         WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON'))
                     SELECT ST_Area(
                                    ST_Difference(
                                            ST_Transform(ST_GeomFromText('{city_boundary_wkt}'), 'EPSG:4326',
                                                         '{metric_crs}'),
                                        ST_Transform((SELECT geom FROM parks_area), 'EPSG:4326', '{metric_crs}')
                                    )
                            ) AS non_park_area_sqm;
                     """
        }
    ]

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    for op in operations:
        required = op['required_files']
        if not all(key in file_paths for key in required):
            continue

        print(f"\nRunning Operation '{op['name']}' for {city_name.title()}.")

        # Dynamically create the dataset name for logging
        dataset_name = f"{city_name.lower()}_{'_'.join(f.replace('_file', '').replace('_wkt', '') for f in required)}.geoparquet"
        formatted_paths = {key: str(path).replace('\\', '/') if isinstance(path, Path) else path for key, path in file_paths.items()}
        sql_query = op['query'].format(metric_crs=metric_crs, **formatted_paths)

        # Cold start run
        with Timer() as t:
            result_df = con.execute(sql_query).df()
        cold_start_time = t.interval
        print(f"Cold start completed in {cold_start_time:.6f}s.")

        # Print a cleaner preview by dropping the WKB column
        print("Result Preview:")
        print(result_df.drop(columns=['geom_wkb'], errors='ignore').head().to_string())

        # Initialize output size as N/A
        output_size_mb = 'N/A'

        # Save GeoParquet output for operations with geometry
        if 'geom_wkb' in result_df.columns:
            result_gdf = gpd.GeoDataFrame(
                result_df.drop(columns=['geom_wkb']),
                geometry=gpd.GeoSeries.from_wkb(result_df['geom_wkb'].apply(bytes)),
                crs="EPSG:4326"
            )

            output_dir = PROCESSED_DATA_DIR / 'duckdb_generated'
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{city_name.lower()}_op_{op['id']}_duckdb.geoparquet"
            result_gdf.to_parquet(output_path)
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
                'use_case': '4. Complex Spatial Join (OSM Data)',
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

    datasets_by_city = {
        'Pinerolo': {
            'neighborhoods': PROCESSED_DATA_DIR / 'pinerolo_neighborhoods.geoparquet',  #
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
            city_boundary_wkt = city_boundary_gdf.geometry.iloc[0].wkt
            print("Boundary fetched successfully.")
        except Exception as e:
            print(f"Could not fetch boundary for {city}. Skipping tests that require it. Error: {e}.")
            city_boundary_wkt = None

        # Print header only once per city
        print(f"\nTesting DuckDB Complex Spatial Join Operations for: {city.upper()}.")

        # Run Op 4.1: Restaurants per Neighborhood
        if paths['neighborhoods'].exists() and paths['restaurants'].exists():
            run_duckdb_complex_spatial_join(
                city_name=city, num_runs=NUMBER_OF_RUNS,
                neighborhoods_file=paths['neighborhoods'],
                restaurants_file=paths['restaurants']
            )
        else:
            print(f"\nERROR: Files for {city} not found. Skipping complex spatial join tests.")

        # Run Op 4.2: Tree Count and Length Sum of Residential Roads Near Hospitals
        if paths['hospitals'].exists() and paths['residential_streets'].exists() and paths['trees'].exists():
            run_duckdb_complex_spatial_join(
                city_name=city, num_runs=NUMBER_OF_RUNS,
                hospitals_file=paths['hospitals'],
                residential_streets_file=paths['residential_streets'],
                trees_file=paths['trees']
            )
        else:
            print(f"\nERROR: Files for {city} not found. Skipping complex spatial join tests.")

        # Run Op 4.3: Area Not Covered by Parks
        if paths['parks'].exists() and city_boundary_wkt:
            run_duckdb_complex_spatial_join(
                city_name=city, num_runs=NUMBER_OF_RUNS,
                parks_file=paths['parks'],
                city_boundary_wkt=city_boundary_wkt
            )
        else:
            print(f"\nERROR: Files for {city} not found. Skipping complex spatial join tests.")

    print("\nAll DuckDB tests for Use Case 4 are complete.")
import duckdb
from pathlib import Path
import sys

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

def run_duckdb_single_table_benchmark(input_file_path):
    """
    Runs a series of single-table geometric analysis benchmarks using DuckDB.
    """
    print(f"Testing DuckDB single table analysis using input file: {input_file_path.name}.")

    # Define the metric CRS for calculations requiring meters (e.g., area, perimeter).
    # EPSG:32632 is WGS 84 / UTM zone 32N, suitable for Northern Italy.
    metric_crs = 'EPSG:32632'

    # A list of dictionaries, where each dictionary defines a specific benchmark operation.
    operations = [
        {
            # Basic aggregation on a transformed geometry.
            'name': '3.1. Average Area (sqm)',
            'query': f"SELECT AVG(ST_Area(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}'))) FROM read_parquet('{{file}}')"
        },
        {
            # Geometric calculation, ordering, and limiting results.
            'name': '3.2. Top 10 Largest Areas (sqm)',
            'query': f"SELECT feature_id, ST_Area(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}')) AS area_sqm FROM read_parquet('{{file}}') ORDER BY area_sqm DESC LIMIT 10"
        },
        {
            # Another aggregation (SUM) but on a different geometric property (perimeter).
            'name': '3.3. Total Perimeter (m)',
            'query': f"SELECT SUM(ST_Perimeter(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}'))) FROM read_parquet('{{file}}')"
        },
        {
            # Filtering based on an intrinsic geometric property (number of vertices).
            'name': '3.4. Select by Vertex Count',
            'query': "SELECT COUNT(*) FROM read_parquet('{file}') WHERE ST_NPoints(geometry) > 50"
        },
        {
            # Geometric calculation (centroid) followed by numeric aggregation.
            'name': '3.5. Average Centroid Coordinate',
            'query': "SELECT AVG(ST_X(ST_Centroid(geometry))), AVG(ST_Y(ST_Centroid(geometry))) FROM read_parquet('{file}')"
        },
        {
            # A chain of two geometric operations (transform, buffer) followed by aggregation (SUM).
            'name': '3.6. Total Buffered Area (sqm)',
            'query': f"SELECT SUM(ST_Area(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}'), 10.0))) FROM read_parquet('{{file}}')"
        },
        {
            # A more complex query with a subquery (CTE) to find lengthened features.
            'name': '3.7. Top 10 by Bounding Box Ratio',
            'query': """
                WITH BBoxes AS (
                    SELECT 
                        feature_id,
                        ST_XMax(geometry) - ST_XMin(geometry) as width,
                        ST_YMax(geometry) - ST_YMin(geometry) as height
                    FROM read_parquet('{file}')
                    WHERE ST_GeometryType(geometry) = 'POLYGON'
                )
                SELECT feature_id, height / width AS ratio
                FROM BBoxes
                WHERE width > 0
                ORDER BY ratio DESC
                LIMIT 10;
            """
        },
        {
            # Categorization using a CASE statement on a calculated property.
            'name': '3.8. Count by Area Class',
            'query': f"""
                SELECT 
                    SUM(CASE WHEN area < 100 THEN 1 ELSE 0 END) AS small_count,
                    SUM(CASE WHEN area >= 100 AND area < 500 THEN 1 ELSE 0 END) AS medium_count,
                    SUM(CASE WHEN area >= 500 THEN 1 ELSE 0 END) AS large_count
                FROM (SELECT ST_Area(ST_Transform(geometry, 'EPSG:4326', '{metric_crs}')) AS area FROM read_parquet('{{file}}'))
            """
        },
        {
            # Measures shape irregularity by comparing a polygon's area to its convex hull's area.
            'name': '3.9. Top 5 Least Convex',
            'query': """
                SELECT feature_id, ST_Area(geometry) / ST_Area(ST_ConvexHull(geometry)) AS convexity
                FROM read_parquet('{file}')
                WHERE ST_GeometryType(geometry) = 'POLYGON' AND ST_Area(ST_ConvexHull(geometry)) > 0
                ORDER BY convexity ASC
                LIMIT 5
            """
        },
        {
            # Tests the query optimizer as the COUNT(*) doesn't require the ST_Simplify operation,
            # so a smart engine will skip it, resulting in a very fast execution time.
            'name': '3.10. Simplify Geometries',
            'query': "SELECT count(*) FROM (SELECT ST_Simplify(geometry, 1.0) FROM read_parquet('{file}'))"
        }
    ]

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    # Loop through each defined operation, run it, and save the results
    for op in operations:
        print(f"\nRunning operation: '{op['name']}'.")

        sql_query = op['query'].format(file=str(input_file_path).replace('\\', '/'))

        with Timer() as t:
            result_df = con.execute(sql_query).df()

        print(f"Operation completed in {t.interval:.6f} seconds.")

        # Print a preview of the query result to the console for a sanity check.
        print("Result Preview:")
        print(result_df.to_string(index=False))

        result_data = {
            'use_case': '3. Single Table Analysis (OSM Data)',
            'technology': 'DuckDB Spatial',
            'operation_description': op['name'],
            'test_dataset': input_file_path.name,
            'execution_time_s': t.interval,
            'output_size_mb': 'N/A',
            'notes': f'Query returned {len(result_df)} rows.'
        }
        save_results(result_data)

    con.close()

if __name__ == '__main__':
    current_script_path = Path(__file__).resolve()
    working_root = current_script_path.parent.parent.parent
    input_file = working_root / 'data' / 'processed' / 'milan_buildings_from_italy_pbf.geoparquet'

    if not input_file.exists():
        print(f"\nERROR: Input file not found at {input_file}.")
    else:
        run_duckdb_single_table_benchmark(input_file)
        print("\nAll DuckDB tests for Use Case 3 are complete.")
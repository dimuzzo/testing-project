import duckdb
from pathlib import Path
from utils import Timer, save_results

def run_duckdb_analysis(input_file_path, show_preview=False):
    """
    Runs single-table analysis benchmarks using DuckDB on a given GeoParquet file.
    """
    print(f"Testing DuckDB Spatial on {input_file_path.name}.")

    # Define the operations to test
    # We explicitly use the read_parquet() function in the FROM clause
    # to tell DuckDB how to read the .geoparquet file.
    # We wrap the 'geometry' column in ST_Transform to convert coordinates
    # from EPSG:4326 (degrees) to EPSG:32632 (meters) BEFORE calculating the area.
    # This gives us results in square meters.
    operations = {
        'average_area': """
                        SELECT AVG(ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32632'))) AS result
                        FROM read_parquet('{file}')
                        """,
        'top_10_largest': """
                          SELECT feature_id, ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32632')) AS area_sqm
                          FROM read_parquet('{file}')
                          ORDER BY area_sqm DESC LIMIT 10
                          """
    }

    # Connect to an in-memory DuckDB and install the spatial extension
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    for op_name, query_template in operations.items():
        print(f"Running operation: '{op_name}'.")

        # Format the query with the correct file path
        sql_query = query_template.format(file=str(input_file_path))

        with Timer() as t:
            result = con.execute(sql_query).df()

        print(f"Operation completed in {t.interval:.6f} seconds.")

        # Show a preview of the result
        if show_preview:
            print("Result Preview:")
            if op_name == 'average_area':
                # For average area, print the single value
                avg_area = result['result'].iloc[0]
                print(f"Average Area: {avg_area:.2f}.")
            else:
                # For other results, print the DataFrame
                print(result.to_string(index=False))

        # Prepare and save the benchmark result
        result_data = {
            'use_case': '3. Analysis (Single Table)',
            'technology': 'DuckDB Spatial',
            'operation_description': f'Calculate {op_name.replace("_", " ")}',
            'test_dataset': input_file_path.name,
            'execution_time_s': t.interval,
            'output_size_mb': 'N/A',
            'notes': f'Query returned {len(result)} rows.'
        }
        save_results(result_data)

    con.close()

if __name__ == '__main__':
    # Define the input file we want to analyze
    current_script_path = Path(__file__).resolve()
    working_root = current_script_path.parent.parent
    input_file = working_root / 'data' / 'processed' / 'milan_buildings_from_italy_pbf.geoparquet'

    print(f"Running Benchmark: Use Case 3.")
    print(f"Using input file: {input_file}.")

    if not input_file.exists():
        print(f"\nERROR: Input file not found at {input_file}")
        print("Please run a Use Cases 1 & 2 script/notebook first to generate the processed file.")
    else:
        # Run the DuckDB benchmarks
        run_duckdb_analysis(input_file, show_preview=True)
        print("\nAll DuckDB tests for Use Case 3 are complete.")
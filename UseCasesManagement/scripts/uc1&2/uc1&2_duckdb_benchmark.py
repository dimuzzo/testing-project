import quackosm
import osmnx as ox
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
PBF_FILEPATH = RAW_DATA_DIR / 'italy-latest.osm.pbf'

def run_duckdb_ingestion_and_filtering(place_name, num_runs=100):
    """
    Runs the data ingestion and filtering benchmark for a given PBF file and place
    a specified number of times and calculates the average performance.
    """
    place_name_clean = place_name.split(',')[0]
    print(f"Testing DuckDB + QuackOSM ingestion & filtering for {place_name_clean} buildings over {num_runs} runs.")

    if not PBF_FILEPATH.exists():
        print(f"ERROR: PBF file not found at {PBF_FILEPATH}. Aborting.")
        return

    execution_times = []
    successful_runs = 0
    failed_runs = 0
    last_successful_gdf = None  # To store the result for saving later

    try:
        # Get the boundary once before the loop to avoid repeated network calls
        print(f"Fetching boundary for {place_name}...")
        boundary_gdf = ox.geocode_to_gdf(place_name)
        print("Boundary fetched successfully.")
    except Exception as e:
        print(f"Could not fetch boundary for {place_name}. Error: {e}. Aborting benchmark.")
        return

    for i in range(num_runs):
        try:
            with Timer() as t:
                # The PbfFileReader is re-initialized on each run to ensure
                # we are measuring the full process, though caching may still occur.
                pbf_reader = quackosm.PbfFileReader(
                    geometry_filter=boundary_gdf.geometry.iloc[0],
                    tags_filter={'building': True}
                )
                features_gdf = pbf_reader.convert_pbf_to_geodataframe(PBF_FILEPATH)

            execution_times.append(t.interval)
            successful_runs += 1
            last_successful_gdf = features_gdf  # Save the result of the last successful run
            # Using carriage return '\r' to print on the same line for cleaner output
            print(f"Run {i + 1}/{num_runs} completed successfully in {t.interval:.4f}s.", end='\r')

        except Exception as e:
            failed_runs += 1
            print(f"\nRun {i + 1}/{num_runs} failed. Error: {e}")

    # Print a newline to move past the progress indicator line
    print("\n")

    # After the loop, calculate and save results
    print("Benchmark run summary:")
    print(f"Successful runs: {successful_runs}")
    print(f"Failed runs: {failed_runs}")

    if successful_runs > 0:
        average_time = sum(execution_times) / len(execution_times)
        print(f"Average execution time: {average_time:.4f}s.")

        output_size_mb = 'N/A'
        if last_successful_gdf is not None:
            num_features = len(last_successful_gdf)  # Get the number of features
            output_filename = f"{place_name_clean.lower()}_buildings_duckdb.geoparquet"
            output_path = PROCESSED_DATA_DIR / output_filename
            PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

            print(f"Saving output file {output_filename} to calculate its size.")
            last_successful_gdf.to_parquet(output_path)

            output_size_bytes = output_path.stat().st_size
            output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"
            print(f"Output file size: {output_size_mb} MB.")
            print(f"Number of features found: {num_features}")

        # Save the aggregated result
        result_data = {
            'use_case': '1&2. Ingestion & Filtering (OSM Data)',
            'technology': 'DuckDB + Quackosm',
            'operation_description': f'Extract {place_name_clean} buildings from PBF',
            'test_dataset': PBF_FILEPATH.name,
            'execution_time_s': average_time,
            'num_runs': successful_runs,
            'output_size_mb': output_size_mb,
            'notes': f'Found {num_features} buildings. Average time over {successful_runs} successful runs for {place_name_clean}.'
        }
        save_results(result_data)
        print("Aggregated results saved successfully.")
    else:
        print("No successful runs to save.")

if __name__ == '__main__':
    # This is the only variable you can to change to test a different place.
    PLACE_TO_BENCHMARK = "Milan, Italy"
    NUMBER_OF_RUNS = 100

    run_duckdb_ingestion_and_filtering(place_name=PLACE_TO_BENCHMARK, num_runs=NUMBER_OF_RUNS)
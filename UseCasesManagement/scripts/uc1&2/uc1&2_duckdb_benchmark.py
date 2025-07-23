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
        print(f"ERROR: PBF file not found at {PBF_FILEPATH}. Aborting the tests.")
        return

    try:
        print(f"Fetching boundary for {place_name}.")
        boundary_gdf = ox.geocode_to_gdf(place_name)
        print("Boundary fetched successfully.")
    except Exception as e:
        print(f"Could not fetch boundary for {place_name}. Error: {e}. Aborting benchmark.")
        return

    pbf_reader = quackosm.PbfFileReader(
        geometry_filter=boundary_gdf.geometry.iloc[0],
        tags_filter={'building': True}
    )

    cold_start_time = None
    hot_start_times = []
    last_successful_gdf = None

    # Cold start run
    print("\nRunning Cold Start (First run).")
    try:
        with Timer() as t:
            features_gdf = pbf_reader.convert_pbf_to_geodataframe(PBF_FILEPATH)
        cold_start_time = t.interval
        last_successful_gdf = features_gdf
        print(f"Cold start completed in {cold_start_time:.4f}s.")
    except Exception as e:
        print(f"Cold start run failed. Error: {e}. Aborting subsequent runs.")
        return  # Abort if the first run fails

    # Hot start runs
    if num_runs > 1:
        print("\nRunning Hot Starts (Second to last run).")
        for i in range(num_runs - 1):
            try:
                with Timer() as t:
                    # The PBF reader object is reused, benefiting from internal/OS caching
                    hot_features_gdf = pbf_reader.convert_pbf_to_geodataframe(PBF_FILEPATH)
                hot_start_times.append(t.interval)
                print(f"Run {i + 2}/{num_runs} (Hot) completed in {t.interval:.4f}s.", end='\r')
            except Exception as e:
                print(f"\nHot run {i + 2}/{num_runs} failed. Error: {e}")
        print("\n")  # Newline after progress indicator

    # After the loop, calculate and save results
    print("Benchmark run summary:")
    # Process and save cold start result
    if cold_start_time is not None:
        print(f"Cold start time: {cold_start_time:.4f}s.")
        num_features = len(last_successful_gdf)

        # Calculate size (only once)
        output_filename = f"{place_name_clean.lower()}_buildings_duckdb.geoparquet"
        output_path = PROCESSED_DATA_DIR / output_filename
        PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        last_successful_gdf.to_parquet(output_path)
        output_size_bytes = output_path.stat().st_size
        output_size_mb = f"{(output_size_bytes / (1024 * 1024)):.4f}"

        # Save cold start result
        cold_result = {
            'use_case': '1&2. Ingestion & Filtering (OSM Data)',
            'technology': 'DuckDB + Quackosm',
            'operation_description': f'Extract {place_name_clean} buildings from PBF',
            'test_dataset': PBF_FILEPATH.name,
            'execution_time_s': cold_start_time,
            'num_runs': 1,
            'output_size_mb': output_size_mb,
            'notes': f'Found {num_features} buildings for {place_name_clean}. Cold start (first run).'
        }
        save_results(cold_result)
        print(f"Cold start result saved. Features: {num_features}, Size: {output_size_mb} MB.")

    # Process and save hot start results
    if len(hot_start_times) > 0:
        average_hot_time = sum(hot_start_times) / len(hot_start_times)
        print(f"Average hot start time: {average_hot_time:.4f}s over {len(hot_start_times)} runs.")

        # Save hot start average result
        hot_result = {
            'use_case': '1&2. Ingestion & Filtering (OSM Data)',
            'technology': 'DuckDB + Quackosm',
            'operation_description': f'Extract {place_name_clean} buildings from PBF',
            'test_dataset': PBF_FILEPATH.name,
            'execution_time_s': average_hot_time,
            'num_runs': len(hot_start_times),
            'output_size_mb': output_size_mb,  # Reuse size from cold start
            'notes': f'Found {num_features} buildings for {place_name_clean}. Average of {len(hot_start_times)} hot cache runs.'
        }
        save_results(hot_result)
        print("Hot start average result saved.")
    else:
        print("No successful runs to save.")

if __name__ == '__main__':
    # This is the only variable you can to change to test a different place.
    PLACE_TO_BENCHMARK = "Milan, Italy"
    NUMBER_OF_RUNS = 100

    run_duckdb_ingestion_and_filtering(place_name=PLACE_TO_BENCHMARK, num_runs=NUMBER_OF_RUNS)
import os
import quackosm
import osmnx as ox
from pathlib import Path
from utils import Timer, save_results

def run_quackosm_ingestion(pbf_filename, place_name, output_filename):
    """
    Runs the data ingestion and filtering benchmark for a given PBF file and place.
    """
    # Define robust paths using pathlib
    current_script_path = Path(__file__).resolve()
    working_root = current_script_path.parent.parent  # Navigates up to the 'UseCasesManagement' folder

    pbf_path = working_root / 'data' / 'raw' / pbf_filename
    output_dir = working_root / 'data' / 'processed'
    output_file = output_dir / output_filename

    print(f"Starting ingestion for '{place_name}' from: {pbf_path}.")

    # Run and time the main ingestion logic
    with Timer() as t:
        # Fetch the geographical boundary for the city
        area_gdf = ox.geocode_to_gdf(place_name)

        # Configure the PBF reader with filters and low-memory settings
        pbf_reader = quackosm.PbfFileReader(
            geometry_filter=area_gdf.geometry.iloc[0],
            tags_filter={'building': True},
            row_group_size=10000
        )

        # Execute the conversion
        buildings_gdf = pbf_reader.convert_pbf_to_geodataframe(str(pbf_path))

        # Create output directory and save the resulting GeoParquet file
        os.makedirs(output_dir, exist_ok=True)
        buildings_gdf.to_parquet(output_file)

    print(f"Ingestion for '{place_name}' completed in {t.interval:.4f} seconds.")

    # Calculate output file size
    # Get file size in bytes and convert to MegaBytes (MB)
    output_size_bytes = output_file.stat().st_size
    output_size_mb = output_size_bytes / (1024 * 1024)
    print(f"Output file size: {output_size_mb:.2f} MB.")

    # Prepare and save the benchmark results
    result_data = {
        'use_case': '1. Ingestion & 2. Filtering',
        'technology': 'QuackOSM',
        'operation_description': f'Extract and save buildings for {place_name}',
        'test_dataset': pbf_path.name,
        'execution_time_s': t.interval,
        'output_size_mb': f"{output_size_mb:.2f}",
        'notes': f'Found {len(buildings_gdf)} buildings. Forced row_group_size=10000.'
    }
    # Call the helper function from utils.py to write to the CSV
    save_results(result_data)

if __name__ == '__main__':
    # Define parameters for this specific run
    PBF_FILE = 'italy-latest.osm.pbf'
    PLACE = 'Milan, Italy'
    OUTPUT_FILENAME = 'milan_buildings_from_italy_pbf.geoparquet'

    print(f"Running Benchmark: Use Cases 1 & 2 (PBF: {PBF_FILE}).")

    # Use a try/except block to handle potential crashes
    try:
        run_quackosm_ingestion(PBF_FILE, PLACE, OUTPUT_FILENAME)
        print("Benchmark for Use Cases 1 & 2 finished successfully.")
    except Exception as e:
        print(f"\nERROR: The script failed with an exception.")
        print(f"Error details: {e}.")
        import traceback
        traceback.print_exc()
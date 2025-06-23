import quackosm
import os
import urllib.request
from pathlib import Path

def run_simple_conversion_test():
    # --- Setup paths ---
    current_script_path = Path(__file__).resolve()
    project_root = current_script_path.parent.parent.parent
    raw_data_dir = project_root / 'data' / 'raw'

    pbf_url = "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"
    pbf_filename = pbf_url.split('/')[-1]
    pbf_path = raw_data_dir / pbf_filename

    os.makedirs(raw_data_dir, exist_ok=True)

    if not pbf_path.exists():
        print(f"Test file not found. Downloading from {pbf_url}...")
        urllib.request.urlretrieve(pbf_url, pbf_path)
        print(f"Download complete. File saved to {pbf_path}")
    else:
        print(f"Test file already exists at {pbf_path}.")

    print("\n" + "-" * 30)
    print(f"Attempting the simplest possible conversion for: {pbf_path}")
    print("-" * 30)

    try:
        # The function takes the INPUT path and returns the OUTPUT path.
        # We still need the tags_filter workaround.
        created_parquet_path = quackosm.convert_pbf_to_parquet(
            pbf_path=str(pbf_path),
            tags_filter={}
        )

        print(f"\nSUCCESS! The simplest conversion worked without crashing.")
        print(f"The output file has been created at: {created_parquet_path}")

    except Exception as e:
        print(f"\nERROR! The conversion failed with a clear Python exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    run_simple_conversion_test()
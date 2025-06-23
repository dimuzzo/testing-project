import csv
import os
import time
from pathlib import Path

# Path Setup
# Calculate the correct path for the results file, relative to this script's location
# The results folder is in the parent directory of the 'scripts' folder
WORKING_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = WORKING_ROOT / 'results'
RESULTS_FILE = RESULTS_DIR / 'benchmark_results.csv'

# CSV Header
CSV_HEADER = [
    'use_case',
    'technology',
    'operation_description',
    'test_dataset',
    'execution_time_s',
    'notes'
]


def save_results(result_data):
    """
    Saves a result row to the main CSV file.
    Creates the results directory and the file header if they don't exist.
    """
    try:
        # Ensure the results directory exists
        os.makedirs(RESULTS_DIR, exist_ok=True)

        # Check if the file exists to decide whether to write the header
        file_exists = os.path.isfile(RESULTS_FILE)

        with open(RESULTS_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADER)

            if not file_exists:
                writer.writeheader()  # Write header only if the file is new

            writer.writerow(result_data)

        print(f"Result saved for {result_data['technology']}: {result_data['execution_time_s']:.4f}s")
    except Exception as e:
        print(f"ERROR: Could not save results to {RESULTS_FILE}. Details: {e}")


class Timer:
    """A simple context manager timer."""

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start
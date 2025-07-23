import csv
import time
from pathlib import Path

class Timer:
    """A simple context manager timer."""
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

def save_results(result_data, results_file='benchmark_results.csv'):
    """
    Saves a dictionary of benchmark results to a specified CSV file.
    """
    # Define the output path relative to this utility script
    results_dir = Path(__file__).resolve().parent.parent / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    results_filepath = results_dir / results_file

    # Field names for the CSV file headers
    fieldnames = [
        'use_case', 'technology', 'operation_description', 'test_dataset',
        'execution_time_s', 'num_runs', 'output_size_mb', 'notes'
    ]

    # Check if the file exists to write headers only once
    file_exists = results_filepath.exists()

    with open(results_filepath, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        # Prepare a dictionary with all required fields, providing defaults
        row_dict = {field: result_data.get(field, 'N/A') for field in fieldnames}
        writer.writerow(row_dict)
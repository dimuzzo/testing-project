import rasterio
import rasterio.mask
# import geopandas as gpd, wrote just as a reminder of the technologies used in the project
import osmnx as ox
# import duckdb, wrote just as a reminder of the technologies used in the project
import psycopg2
from pathlib import Path
import sys
import os

# Add the parent directory of 'scripts' to the Python path to find 'utils'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_utils import Timer, save_results

# Global Path Setup
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent.parent
RASTER_PATH = WORKING_ROOT / 'data' / 'raw' / 'raster' / 'w49540_s10.tif'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'

# This section serves as a placeholder to maintain a consistent benchmark structure.
# It explicitly documents that DuckDB currently lacks native raster support,
# so these tests are skipped. The results are recorded as 0.0 for completeness.
def run_duckdb_benchmark():
    # Use Case 1: Ingestion
    print("\nTesting DuckDB Spatial ingestion for raster data.")
    print("SKIPPING: DuckDB Spatial currently does not have native support for raster data.")
    save_results({
        'use_case': '1. Ingestion (Raster Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Load GeoTIFF file',
        'test_dataset': RASTER_PATH.name,
        'execution_time_s': 0.0,
        'output_size_mb': 0.0,
        'notes': 'Technology not supported for this data type.'
    })

    # Use Case 2: Filtering
    print("\nTesting DuckDB Spatial filtering for raster data.")
    print("SKIPPING: DuckDB Spatial currently does not have native support for raster data.")
    save_results({
        'use_case': '2. Filtering (Raster Data)',
        'technology': 'DuckDB Spatial',
        'operation_description': 'Clip raster with a vector polygon',
        'test_dataset': RASTER_PATH.name,
        'execution_time_s': 0.0,
        'output_size_mb': 0.0,
        'notes': 'Technology not supported for this data type.'
    })

def run_postgis_benchmark():
    # This test measures the performance of a server-side raster clipping operation.
    # It assumes the 'dem_table' was pre-loaded with the raster2pgsql tool.
    conn = None
    try:
        conn = psycopg2.connect(dbname="osm_benchmark_db", user="postgres", password="postgres", host="localhost")
        # Use Case 2: Filtering
        print("\nTesting PostGIS filtering for raster data.")
        area_gdf = ox.geocode_to_gdf("Turin, Italy")
        # The vector polygon geometry is converted to Well-Known Text (WKT) format
        # so it can be embedded directly into the SQL query string.
        area_wkt = area_gdf.geometry.iloc[0].wkt

        # This query performs a complete server-side raster clipping.
        # Here is a breakdown of the key PostGIS functions used:
        # - ST_Intersects: Used in the WHERE clause as a fast spatial filter. It ensures that the
        #   expensive clipping operation is only performed on raster tiles that actually overlap
        #   with the Turin boundary, improving performance.
        # - ST_Clip: This is the core function that cuts the raster `rast` using the provided
        #   vector geometry.
        # - ST_Transform(ST_SetSRID(ST_GeomFromText(...))): This nested block is crucial for
        #   correctness. It takes the WKT of the Turin polygon, sets its native CRS to 4326,
        #   and then transforms it on-the-fly to match the raster's CRS (ST_SRID(rast)).
        # - ST_AsGDALRaster: After clipping, the resulting raster is converted back into a
        #   standard format ('GTiff') that the Python client can receive and process.
        query = f"""
        SELECT ST_AsGDALRaster(ST_Clip(rast, ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast)) ), 'GTiff') 
        FROM public.dem_table
        WHERE ST_Intersects(rast, ST_Transform(ST_SetSRID(ST_GeomFromText('{area_wkt}'), 4326), ST_SRID(rast)));
        """

        with Timer() as t:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

        print(f"PostGIS raster clip completed in {t.interval:.4f}s.")
        save_results({
            'use_case': '2. Filtering (Raster Data)',
            'technology': 'PostGIS',
            'operation_description': 'Clip raster with a vector polygon using ST_Clip',
            'test_dataset': RASTER_PATH.name,
            'execution_time_s': t.interval,
            'output_size_mb': 'N/A',
            'notes': 'Query Time only. Assumes data was pre-loaded.'
        })

    except psycopg2.OperationalError as e:
        print("SKIPPING PostGIS test: Could not connect to database. Please check connection details and ensure the server is running.")
        print(f"Error: {e}.")
    except psycopg2.errors.UndefinedTable:
        print(f"SKIPPING PostGIS test: Table 'dem_table' not found.")
        print(f"Please run the 'raster2pgsql' command first to import the data.")
    except Exception as e:
        print(f"An error occurred during PostGIS test: {e}.")
    finally:
        if conn:
            conn.close()

def run_python_raster_benchmark():
    # Use Case 1: Ingestion
    print("\nTesting Python ingestion for raster data.")
    with Timer() as t:
        # rasterio.open() is a lazy-loading operation. It's extremely fast because
        # it only reads the file's metadata (CRS, bounds, dimensions) and opens a
        # file handle, without loading the large pixel array into RAM.
        with rasterio.open(RASTER_PATH) as src:
            notes = f'Opened file with {src.count} band(s) ({src.width}x{src.height}px).'

    save_results({
        'use_case': '1. Ingestion (Raster Data)',
        'technology': 'Python (Rasterio)',
        'operation_description': 'Open GeoTIFF file handle',
        'test_dataset': RASTER_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': 'N/A',
        'notes': notes
    })

    # Use Case 2: Filtering
    print("\nTesting Python filtering for raster data.")
    boundary_gdf = ox.geocode_to_gdf("Turin, Italy")

    with Timer() as t:
        with rasterio.open(RASTER_PATH) as src:
            # The vector geometry's CRS must be transformed to match the raster's CRS.
            # Failure to do this results in a "shapes do not overlap" error.
            boundary_gdf = boundary_gdf.to_crs(src.crs)
            # rasterio.mask.mask reads the necessary chunks of the raster from disk,
            # applies the vector mask in memory and returns the resulting image array.
            out_image, out_transform = rasterio.mask.mask(src, boundary_gdf.geometry, crop=True)
            out_meta = src.meta

    output_path = PROCESSED_DATA_DIR / 'turin_dem_clipped.tif'
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    out_meta.update({"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})
    with rasterio.open(output_path, "w", **out_meta) as dest:
        dest.write(out_image)

    output_size_mb = f"{(output_path.stat().st_size / (1024 * 1024)):.4f}"

    save_results({
        'use_case': '2. Filtering (Raster Data)',
        'technology': 'Python (Rasterio)',
        'operation_description': 'Clip raster with a vector polygon',
        'test_dataset': RASTER_PATH.name,
        'execution_time_s': t.interval,
        'output_size_mb': output_size_mb,
        'notes': 'Clipped DEM to Turin boundary.'
    })

if __name__ == '__main__':
    print(f"Running Raster Data Benchmark.")
    print(f"Using input file: {RASTER_PATH.name}.")

    if not RASTER_PATH.exists():
        print(f"\nERROR: Input file not found at {RASTER_PATH.name}.")
        print("Please download the correct DEM Raster file and place it in the correct folder.")
    else:
        run_duckdb_benchmark()
        run_postgis_benchmark()
        run_python_raster_benchmark()
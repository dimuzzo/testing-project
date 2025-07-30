import geopandas as gpd
from pathlib import Path
import rioxarray
from sqlalchemy import create_engine
import sys

# Assumes benchmark_utils.py is in the parent directory of this script's location
sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark_utils import Timer

print("Starting Data Preparation for Use Case 5.")

# Path to processed data
CURRENT_SCRIPT_PATH = Path(__file__).resolve()
WORKING_ROOT = CURRENT_SCRIPT_PATH.parent.parent
RAW_DATA_DIR = WORKING_ROOT / 'data' / 'raw'
PROCESSED_DATA_DIR = WORKING_ROOT / 'data' / 'processed'
VECTOR_INPUT_PATH = RAW_DATA_DIR / 'comuni_istat' / 'Com01012025_WGS84.shp'
RASTER_INPUT_PATH = RAW_DATA_DIR / 'raster' / 'GHS_POP_ITALY_100m.tif'
VECTOR_OUTPUT_PATH = PROCESSED_DATA_DIR / 'postgis_generated' / 'comuni_istat_clean.gpkg'

# Database connection string
# Format: "postgresql://user:password@host:port/dbname"
DB_CONNECTION_URL = "postgresql://postgres:postgres@localhost:5432/osm_benchmark_db"
TABLE_NAME = 'comuni_istat_clean'

try:
    print(f"Reading original shapefile.")
    gdf = gpd.read_file(VECTOR_INPUT_PATH)
    print(f"Read {len(gdf)} features.")

    print("Applying .buffer(0) to fix geometries.")
    gdf.geometry = gdf.geometry.buffer(0)

    print("Reading target CRS from raster.")
    target_crs = rioxarray.open_rasterio(RASTER_INPUT_PATH).rio.crs

    print(f"Reprojecting {len(gdf)} features to target CRS.")
    gdf_reprojected = gdf.to_crs(target_crs)
    print("Reprojection complete.")

    print(f"Saving clean data to: {VECTOR_OUTPUT_PATH.name}.")
    VECTOR_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    gdf_reprojected.to_file(VECTOR_OUTPUT_PATH, driver='GPKG')

    print("\nData Preparation Complete.")
    print(f"Clean file ready at: {VECTOR_OUTPUT_PATH}.")

    print("\nStarting Data Loading into PostGIS.")

    print(f"Connecting to the database.")
    engine = create_engine(DB_CONNECTION_URL)

    print(f"Loading {len(gdf_reprojected)} clean features into table 'vector_data.{TABLE_NAME}'...")
    with Timer() as t:
        gdf_reprojected.to_postgis(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace",
            index=True,
            index_label="id",
            schema="vector_data"
        )
    print(f"Loading complete in {t.interval:.2f} seconds.")
    print(f"Table 'public.{TABLE_NAME}' is now ready in your database.")

except Exception as e:
    print(f"\nAn error occurred: {e}.")
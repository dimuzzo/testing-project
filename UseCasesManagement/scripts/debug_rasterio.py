import rasterio

raster_path = '.../testing-project/UseCasesManagement/data/raw/raster/w49540_s10.tif' # The complete path needs to be inserted to make it work, naturally

try:
    with rasterio.open(raster_path) as src:
        print(f"File: {raster_path}")
        print(f"CRS: {src.crs}")
        print(f"Band's number: {src.count}")
        print(f"Dimensions: {src.width}x{src.height}")
        print(f"Data types: {src.dtypes[0]}")
except Exception as e:
    print(f"Error while opening the file: {e}")
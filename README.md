# Testing Project by Alessandro
Testing libraries and databases like GeoPandas, OSMnx, Rasterio and DuckDB. A lot of documentation is going to be uploaded...

---

## 🗺 1. GeoPandas - Analysis and manipulation of vector geographic data

### GitHub Repo
https://github.com/geopandas/geopandas

### Official Site
https://geopandas.org/

### Tutorial on YouTube
List of Videos: https://www.youtube.com/watch?v=slqZVgB8tIg&list=PLLxyyob7YmEEbXc1R6Tc5YvVIAYPuvoMY

Beginner's Guide: https://youtu.be/t7lliJXFt8w?si=cgZfXHD51c-dLSgV

### How to Install
bash:

      pip install geopandas

### Small Example
python:

      import geopandas as gpd
   
      world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
   
      world.plot()

---

## 🛣 2. OSMnx - Download and view data from OpenStreetMap

### GitHub Repo
https://github.com/gboeing/osmnx

https://github.com/gboeing/osmnx-examples

### Official Site
https://osmnx.readthedocs.io/

### Tutorial on YouTube
List of Videos: https://www.youtube.com/watch?v=CgW0HPHqFE8&list=PLbabOmM8ZAJBXOARP4EXd3vrJ7R6pUnSx

### How to Install
bash:

      pip install osmnx

### Small Example
python:

      import osmnx as ox
   
      graph = ox.graph_from_place("Bologna, Italy", network_type="walk")
   
      ox.plot_graph(ox.project_graph(graph))

---

## 🛰 3. Rasterio - Working with raster data and satellite imagery

### GitHub Repo
https://github.com/rasterio/rasterio

### Official Site
https://rasterio.readthedocs.io/

### Tutorial on YouTube
Beginner's Guide: https://youtu.be/LVt8CezezZQ?si=QmbTTG2S9PZNttDv

GeoTIFF + Rasterio Tutorial: https://youtu.be/ieyODuIjXp4?si=7In_IOQWZodHGlmI

### How to Install
bash:

      pip install rasterio

### Small Example
python:

      import rasterio
   
      url = "https://github.com/mapbox/rasterio/raw/main/tests/data/RGB.byte.tif"
   
      with rasterio.open(url) as src:
      
          print(src.count, src.crs, src.bounds)

---

## 🐥 4. DuckDB with spatial extension — Embedded SQL database with GIS support

### GitHub Repo
https://github.com/duckdb/duckdb

### Official Site
https://duckdb.org/docs/stable/extensions/spatial/overview.html

### Tutorial on YouTube
List of Videos: https://www.youtube.com/watch?v=ZX5FdqzGT1E&list=PLIYcNkSjh-0ztvwoAp3GeW8HNSUSk_q3K

### How to Install
bash:

      pip install duckdb

### Small Example
python:   
      
      import duckdb

      duckdb.install_extension("spatial")
   
      duckdb.load_extension("spatial")
   
      duckdb.query("SELECT ST_Buffer(ST_Point(1, 1), 10)").fetchall()

---

## 🎁 Additional links: Geographic data to implement

### CORINE Land Cover (Copernicus):

https://land.copernicus.eu/en/products/corine-land-cover

### Human Settlement Layer (Copernicus):

http://human-settlement.emergency.copernicus.eu/datasets.php

### OpenStreetMap Data (via OSMnx or GeoFabrik):

https://download.geofabrik.de/

### Some Videos to Check:

1. https://www.youtube.com/watch?v=0mWgVVH_dos
2. https://youtu.be/jJD_xkU5d1o?si=7H7cE6gEb8uCZ8ZU

---
# Testing Project by Dimuzzo
Testing libraries and databases like GeoPandas, OSMnx, Rasterio, DuckDB and PostGIS. A lot of documentation is going to be uploaded...

---

## 🗺 1. GeoPandas - Analysis and manipulation of vector geographic data

### 🔗 GitHub Repo
👉 https://github.com/geopandas/geopandas

### 🌐 Official Site
👉 https://geopandas.org/

### 🎥 Tutorial on YouTube
👉 searching...

### 🧰 How to Install
👉 bash:

      pip install geopandas

### 🚀 Small Example
👉 python:

      import geopandas as gpd
   
      world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
   
      world.plot()

---

## 🛣 2. OSMnx - Download and view data from OpenStreetMap

### 🔗 GitHub Repo
👉 https://github.com/gboeing/osmnx

### 🌐 Official Site
👉 https://osmnx.readthedocs.io/

### 🎥 Tutorial on YouTube
👉 searching...

### 🧰 How to Install
👉 bash:

      pip install osmnx

### 🚀 Small Example
👉 python:

      import osmnx as ox
   
      graph = ox.graph_from_place("Bologna, Italy", network_type="walk")
   
      ox.plot_graph(ox.project_graph(graph))

---

## 🛰 3. Rasterio - Working with raster data and satellite imagery

### 🔗 GitHub Repo
👉 https://github.com/rasterio/rasterio

### 🌐 Official Site
👉 https://rasterio.readthedocs.io/

### 🎥 Tutorial on YouTube
👉 searching...

### 🧰 How to Install
👉 bash:

      pip install rasterio

### 🚀 Small Example
👉 python:

      import rasterio
   
      url = "https://github.com/mapbox/rasterio/raw/main/tests/data/RGB.byte.tif"
   
      with rasterio.open(url) as src:
   
          print(src.count, src.crs, src.bounds)

---

## 🐥 4. DuckDB with spatial extension — Embedded SQL database with GIS support

### 🔗 GitHub Repo
👉 https://github.com/duckdb/duckdb

### 🌐 Official Site
👉 https://duckdb.org/docs/stable/extensions/spatial/overview.html

### 🎥 Tutorial on YouTube
👉 searching...

### 🧰 How to Install
👉 bash:

      pip install duckdb

### 🚀 Small Example
👉 python:   
      
      import duckdb

      duckdb.install_extension("spatial")
   
      duckdb.load_extension("spatial")
   
      duckdb.query("SELECT ST_Buffer(ST_Point(1, 1), 10)").fetchall()

---

## 🐘 5. PostGIS with Docker - Postgres + GIS Extensions

### 🔗 GitHub Repo
👉 https://github.com/postgis/docker-postgis

### 🌐 Official Site
👉 https://postgis.net/

### 🎥 Tutorial on YouTube
👉 searching...

### 🧰 How to Install
👉 Docker Desktop: https://www.docker.com/products/docker-desktop/

👉 pgAdmin: https://www.pgadmin.org/

### 🚀 Small Example
👉 Connect to pgAdmin or psql:

      CREATE EXTENSION postgis;
   
      SELECT PostGIS_Version();

---

## 🎁 Bonus: Geographic data to implement

### 🌍 CORINE Land Cover (Copernicus):

👉 https://land.copernicus.eu/en/products/corine-land-cover

### 🏙 Human Settlement Layer (Copernicus):

👉 http://human-settlement.emergency.copernicus.eu/datasets.php

### 🗺 OpenStreetMap Data (via OSMnx or GeoFabrik):

👉 https://download.geofabrik.de/





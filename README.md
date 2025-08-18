# Testing Project by Alessandro

![GitHub last commit](https://img.shields.io/github/last-commit/dimuzzo/testing-project?style=flat-square&logo=github&label=Last%20Commit)
![GitHub repo size](https://img.shields.io/github/repo-size/dimuzzo/testing-project?style=flat-square&logo=github&label=Repo%20Size)
![GitHub stars](https://img.shields.io/github/stars/dimuzzo/testing-project?style=flat-square&logo=github&label=Stars)

---

## 📊 Analyzation Part - Testing libraries and databases like GeoPandas, OSMnx, Rasterio and DuckDB (+ QuackOSM)

The first part of this repository contains the Jupyter Notebooks for each library, database or tool tested, as the primary goal is to understand how all of them work and which are their limits during **Heavy Benchmarking** tests.

### 🗺 1. GeoPandas - Analysis and manipulation of vector geographic data

#### GitHub Repo
- https://github.com/geopandas/geopandas

#### Official Site
- https://geopandas.org/

#### Tutorial on YouTube
- List of Videos: https://www.youtube.com/watch?v=slqZVgB8tIg&list=PLLxyyob7YmEEbXc1R6Tc5YvVIAYPuvoMY
- Beginner's Guide: https://youtu.be/t7lliJXFt8w?si=cgZfXHD51c-dLSgV

#### How to Install
- via Bash:

        pip install geopandas
- via Python:

        import geopandas as gdp

### 🛣 2. OSMnx - Download and view data from OpenStreetMap

#### GitHub Repo
- https://github.com/gboeing/osmnx
- https://github.com/gboeing/osmnx-examples

#### Official Site
- https://osmnx.readthedocs.io/

#### Tutorial on YouTube
- List of Videos: https://www.youtube.com/watch?v=CgW0HPHqFE8&list=PLbabOmM8ZAJBXOARP4EXd3vrJ7R6pUnSx

### How to Install
- via Bash:

        pip install osmnx
- via Python:

        import osmnx as ox

### 🛰 3. Rasterio - Working with raster data and satellite imagery

#### GitHub Repo
- https://github.com/rasterio/rasterio

#### Official Site
- https://rasterio.readthedocs.io/

#### Tutorial on YouTube
- Beginner's Guide: https://youtu.be/LVt8CezezZQ?si=QmbTTG2S9PZNttDv
- GeoTIFF + Rasterio Tutorial: https://youtu.be/ieyODuIjXp4?si=7In_IOQWZodHGlmI

#### How to Install
- via Bash:

        pip install rasterio
- via Python:

        import rasterio

### 🐥 4. DuckDB with spatial extension - Embedded SQL database with GIS support

#### GitHub Repo
- https://github.com/duckdb/duckdb

#### Official Site
- https://duckdb.org/docs/stable/extensions/spatial/overview.html

#### Tutorial on YouTube
- List of Videos: https://www.youtube.com/watch?v=ZX5FdqzGT1E&list=PLIYcNkSjh-0ztvwoAp3GeW8HNSUSk_q3K

#### How to Install
- via Bash:

        pip install duckdb
- via Python:
      
        import duckdb

### 🦆 5. QuackOSM - An open-source tool for reading OpenStreetMap PBF files using DuckDB

#### GitHub Repo
- https://github.com/kraina-ai/quackosm

#### Official Site
- https://kraina-ai.github.io/quackosm/0.2.0/

#### Tutorial on YouTube
- Livestreaming from the Developer (Kamil Raczycki): https://www.youtube.com/live/r6cWiSULgYs?si=8zYZciaU-MOGZpeY&t=1928

#### How to Install
- via Bash:

        pip install quackosm
- via Python:
      
        import quackosm as qosm

### 🔗 Additional links 

#### CORINE Land Cover (Copernicus):

- https://land.copernicus.eu/en/products/corine-land-cover

#### Human Settlement Layer (Copernicus):

- http://human-settlement.emergency.copernicus.eu/datasets.php

#### OpenStreetMap Wiki, Data (via OSMnx or GeoFabrik), Community and Tools:

- https://wiki.openstreetmap.org/wiki/Using_OpenStreetMap
- https://wiki.openstreetmap.org/wiki/Relation
- https://wiki.openstreetmap.org/wiki/Map_features
- https://download.geofabrik.de/
- https://github.com/openstreetmap
- https://overpass-turbo.eu/
- https://towardsdatascience.com/how-to-read-osm-data-with-duckdb-ffeb15197390/

#### Some Videos to Check:

- https://www.youtube.com/watch?v=0mWgVVH_dos
- https://youtu.be/jJD_xkU5d1o?si=7H7cE6gEb8uCZ8ZU

---

## 🖥️ Comparison Part - Comparing the perfomance of 3 different Geospatial technologies

The second part of this repository contains the code made and the results obtained to make a **Comparative performance analysis of Geospatial technologies**. 

The goal is to quantitatively measure and qualitatively assess the **trade-offs** between modern, file-based systems and traditional, server-based databases for common Geospatial tasks.

The analysis is structured around the **TDL (Technologies, Data, Libraries)** framework, which clearly distinguishes the core components of each benchmark.

### 📌 Technologies Under Comparison

The benchmark evaluates 3 primary technology stacks:

1.  **DuckDB (+ Spatial Extension)**: A modern, in-process analytical SQL database known for its high speed and efficient handling of columnar data formats like GeoParquet.
2.  **PostgreSQL (+ PostGIS Extension)**: The industry-standard, open-source object-relational database server, renowned for its robustness and comprehensive set of spatial features.
3.  **GeoPandas (Pure Python Stack)**: The leading library for Geospatial analysis in Python, representing a fully in-memory, file-based approach built on `pandas`, `shapely`, and `pyproj`.

### 📋 Benchmark Use Cases

A series of 5 realistic Use Cases were designed to test the technologies on a range of tasks with varying complexity and data scales (using data for Pinerolo, Milan and Rome).

* **Use Case 1 & 2 - Ingestion & Filtering**: Measures the efficiency of reading raw data (from `.pbf`, `.shp`, `.tif`) and extracting specific subsets.
* **Use Case 3 - Single Table Analysis**: Benchmarks performance on single-dataset geometric calculations and aggregations (e.g. Calculating areas, buffers).
* **Use Case 4 - Complex Spatial Joins**: Stress-tests the systems with computationally intensive multi-dataset joins (e.g. Point-In-Polygon, Proximity analysis).
* **Use Case 5 - Vector-Raster Analysis**: Evaluates performance on zonal statistics, a task combining vector and raster data.

### 🗃️ Project Structure & Results

* `/scripts` contains all the Python scripts used to run the benchmarks in a reproducible manner.
* `/results` contains the raw quantitative outputs of the benchmarks in a central `benchmark_results.csv` file.
* The **[GitHub Wiki Page](https://github.com/dimuzzo/testing-project/wiki/Tables)** contains the final, formatted tables with the summarized results and qualitative observations for each Use Case.
  
---

> Created by [dimuzzo](https://github.com/dimuzzo)

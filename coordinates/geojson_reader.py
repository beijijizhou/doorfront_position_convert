from matplotlib import pyplot as plt
import pyproj
import folium
import matplotlib
import mapclassify
import numpy as np
import geopandas as gpd
from shapely import Point
import shapely.geometry as geom
import dask_geopandas as dgpd
import time
import pyogrio
import fiona
from geopandas import GeoDataFrame

def time_function(func):
    """
    A decorator to measure the time taken by a function.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record the start time
        result = func(*args, **kwargs)  # Call the original function
        end_time = time.time()  # Record the end time
        execution_time = end_time - start_time  # Calculate the time difference
        print(f"Function {func.__name__} took {execution_time:.4f} seconds")
        return result
    return wrapper
@time_function
def get_buildings_gdf(geojson_file_path = "./pluto (1).geojson") -> GeoDataFrame:
    """
    Optimized function to read the building data and transform to the target CRS using pyogrio.
    """
    # Use pyogrio to read the file
    
    data = pyogrio.read_dataframe(geojson_file_path)
    # Convert to a GeoDataFrame
    gdf = gpd.GeoDataFrame(data, geometry='geometry')
    # Convert to Dask GeoDataFrame
    dask_gdf = dgpd.from_geopandas(gdf, npartitions=1).compute()
    # Transform the CRS
    return dask_gdf
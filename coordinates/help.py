import numpy as np
import shapely.geometry as geom
from shapely.geometry import Point, LineString
from shapely.geometry import Point, LineString
import geopandas as gpd


import numpy as np
from shapely.geometry import Point, LineString
import geopandas as gpd

def generate_ray(point, heading, distance=1/1600, global_crs='EPSG:4326'):
    """
    Generate a ray from a point of interest in the global CRS.

    Args:
        point (shapely.geometry.Point): The starting point of the ray.
        heading (float): The heading in degrees.
        distance (float): The distance in meters to generate the ray.
        global_crs (str): The CRS of the input point (default is WGS84, EPSG:4326).

    Returns:
        geopandas.GeoSeries: The ray as a GeoSeries in the global CRS.
    """
    # Convert heading to radians
    heading_rad = np.deg2rad(float(heading))

    # Calculate the end point of the ray
    end_point = (
        point.x + distance * np.sin(heading_rad),
        point.y + distance * np.cos(heading_rad),
    )

    # Create a LineString object with the starting and end points
    ray = LineString([point, end_point])

    # Convert the ray to a GeoSeries in the global CRS
    ray_geoseries = gpd.GeoSeries([ray], crs=global_crs)

    return ray_geoseries

def get_intersected_buildings(buildings_gdf, ray):
    """
    Function to filter buildings that intersect with the given ray.

    Parameters:
    - buildings_gdf (gpd.GeoDataFrame): The GeoDataFrame containing the buildings.
    - ray (geopandas.GeoSeries): The ray as a GeoSeries to check intersections.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing the buildings that intersect with the ray.
    """
    try:
        # Ensure the spatial index is up-to-date
        sindex = buildings_gdf.sindex

        # Access the Shapely geometry from the GeoSeries
        ray_geometry = ray.geometry[0]




        # Find intersecting buildings
        intersected_indices = list(sindex.intersection(ray_geometry.bounds))  # Use ray_geometry.bounds
        intersected_buildings = buildings_gdf.iloc[
            [idx for idx in intersected_indices if buildings_gdf.iloc[idx].geometry.intersects(ray_geometry)]
        ]
        print(intersected_buildings)
        # Debugging: Print the results
        # print("Intersected Buildings:", intersected_buildings)
        
        return intersected_buildings

    except Exception as e:
        print(f"Error in get_intersected_buildings: {e}")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame in case of error
    

def get_intersection(ray, intersected_buildings):
    
    return None,0
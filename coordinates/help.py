import numpy as np
import shapely.geometry as geom
from shapely.geometry import Point, LineString
from shapely.geometry import Point, LineString
import geopandas as gpd


import numpy as np
from shapely.geometry import Point, LineString
import geopandas as gpd

intersected_count = 0
non_intersected_count = 0
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


def get_intersected_buildings(ray, buildings_gdf):
    global intersected_count, non_intersected_count
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
        intersected_indices = list(sindex.intersection(
            ray_geometry.bounds))  # Use ray_geometry.bounds
        intersected_buildings = buildings_gdf.iloc[
            [idx for idx in intersected_indices if buildings_gdf.iloc[idx].geometry.intersects(
                ray_geometry)]
        ]
        if not intersected_buildings.empty:
            intersected_count += 1
        else:
            non_intersected_count += 1
        # Debugging: Print the results
        # print(intersected_count, non_intersected_count)
        # print("Intersected Buildings:", intersected_buildings)
        print(intersected_count, non_intersected_count)

        return intersected_buildings

    except Exception as e:
        print(f"Error in get_intersected_buildings: {e}")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame in case of error

from shapely.geometry import Point

def get_closest_intersection_point(ray, gdf):
    # Get intersected buildings
    intersected_buildings = get_intersected_buildings(ray, gdf)

    # Calculate intersection points
    intersections = []
    for idx, building in intersected_buildings.iterrows():
        intersection = ray.geometry[0].intersection(building.geometry)
        if not intersection.is_empty:
            intersections.append(intersection)

    # Extract points from intersections
    intersection_points = []
    for intersection in intersections:
        if intersection.geom_type == "MultiLineString":
            intersection_points.extend(
                [Point(coord) for line in intersection.geoms for coord in line.coords]
            )
        elif intersection.geom_type == "LineString":
            intersection_points.extend(
                [Point(coord) for coord in intersection.coords]
            )
        elif intersection.geom_type == "Point":
            intersection_points.append(intersection)
        elif intersection.geom_type == "MultiPoint":
            intersection_points.extend(list(intersection.geoms))

    # Find the closest intersection point
    if intersection_points:
        ray_start_point = Point(ray.geometry[0].coords[0])

        # Find the closest intersection point
        closest_intersection = min(
            intersection_points, key=lambda x: ray_start_point.distance(x)
        )
        
        return closest_intersection

def get_intersection(ray, gdf):
    intersected_buildings = get_intersected_buildings(ray, gdf)
    return get_closest_intersection_point(ray, intersected_buildings)
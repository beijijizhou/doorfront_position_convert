from shapely.geometry import Point
import numpy as np
import shapely.geometry as geom
import geopandas as gpd
from shapely.geometry import Point, LineString
from geopandas import GeoSeries
from typing import Optional, Tuple
from dask_geopandas import GeoDataFrame

intersected_count = 0
non_intersected_count = 0


def generate_ray(point: Point, heading: float, distance=1/1600, global_crs='EPSG:4326') -> GeoSeries:
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


def get_intersected_buildings(ray:GeoSeries, buildings_gdf:GeoDataFrame) -> GeoDataFrame:
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
        # print(intersected_count, non_intersected_count)
       

        return intersected_buildings

    except Exception as e:
        print(f"Error in get_intersected_buildings: {e}")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame in case of error


def get_closest_intersection_point(ray: GeoSeries, gdf:GeoDataFrame) -> Tuple[Optional[GeoSeries], Optional[Point]]:
    # Get intersected buildings
    intersected_buildings = get_intersected_buildings(ray, gdf)

    # Calculate intersection points and associate them with buildings
    intersections = []
    for idx, building in intersected_buildings.iterrows():
        intersection = ray.geometry[0].intersection(building.geometry)
        if not intersection.is_empty:
            # Store intersection and building
            intersections.append((intersection, building))

    # Extract points from intersections and associate them with buildings
    intersection_points_with_buildings = []
    for intersection, building in intersections:
        if intersection.geom_type == "MultiLineString":
            for line in intersection.geoms:
                for coord in line.coords:
                    intersection_points_with_buildings.append(
                        (Point(coord), building))
        elif intersection.geom_type == "LineString":
            for coord in intersection.coords:
                intersection_points_with_buildings.append(
                    (Point(coord), building))
        elif intersection.geom_type == "Point":
            intersection_points_with_buildings.append((intersection, building))
        elif intersection.geom_type == "MultiPoint":
            for point in intersection.geoms:
                intersection_points_with_buildings.append((point, building))

    # Find the closest intersection point and its associated building
    if intersection_points_with_buildings:
        ray_start_point = Point(ray.geometry[0].coords[0])

        # Find the closest intersection point and building
        closest_intersection_point, closest_building = min(
            intersection_points_with_buildings,
            key=lambda x: ray_start_point.distance(
                x[0])  # Compare distance to the point
        )
        
        return closest_building, closest_intersection_point

    return None, None  # Return None if no intersections are found


def get_intersection(ray: GeoSeries, gdf: GeoDataFrame):
    intersected_buildings = get_intersected_buildings(ray, gdf)
    return get_closest_intersection_point(ray, intersected_buildings)

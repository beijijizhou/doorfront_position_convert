import numpy as np
import shapely.geometry as geom
from shapely.geometry import Point, LineString

def generate_ray(point, heading, distance=500):
    """
    This function generates a ray from a point of interest.
    :param point: a Point object from shapely.geometry.Point class.
    :param heading: the heading in degrees.
    :param distance: the distance in meter to be used to generate the ray from the point of interest.
    """
    # Create a point 500 meters away from the starting point in the desired direction of the ray.
    heading = float(heading)
    end_point = (
        point.x + distance * np.sin(np.deg2rad(heading)),
        point.y + distance * np.cos(np.deg2rad(heading)),
    )
    # Create a LineString object with the starting and end points
    ray = geom.LineString([point, end_point])
    return ray
def integrate_with_lat_lon(lat_lon, heading, distance=500):
    """
    Integrates the generate_ray function with latitude and longitude.
    :param lat_lon: Tuple containing latitude and longitude.
    :param heading: The heading in degrees.
    :param distance: The distance in meters for the ray.
    """
    # Convert lat_lon to a Shapely Point
    point = Point(lat_lon[1], lat_lon[0])  # Note: Point takes (longitude, latitude)
    # Generate the ray
    ray = generate_ray(point, heading, distance)
    return ray
import numpy as np
import shapely.geometry as geom
from shapely.geometry import Point, LineString


def generate_ray(point, heading):
    """
    This function generates a ray from a point of interest.
    :param point: a Point object from shapely.geometry.Point class.
    :param heading: the heading in degrees.
    :param distance: the distance in meter to be used to generate the ray from the point of interest.
    """
    distance = 1/800
    # Create a point 500 meters away from the starting point in the desired direction of the ray.
    heading = float(heading)
    end_point = (
        point.x + distance * np.sin(np.deg2rad(heading)),
        point.y + distance * np.cos(np.deg2rad(heading)),
    )
    # Create a LineString object with the starting and end points
    ray = geom.LineString([point, end_point])
    return ray

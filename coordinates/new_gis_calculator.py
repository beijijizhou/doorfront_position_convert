

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


class TimeFunctionMeta(type):
    def __new__(cls, name, bases, dct):
        # Iterate through the class methods and apply the decorator
        for key, value in dct.items():
            if callable(value):  # Apply decorator only to callable methods
                # Apply the time_function decorator
                dct[key] = time_function(value)
        return super().__new__(cls, name, bases, dct)


class GeolocationCalculator():
    """
    This class calculates the distance and the intersection point of a ray from a point of interest with the nearest building.

    """
    @time_function
    def __init__(self, file_path):
        """
        :param file_path: path to the file containing the buildings information with the extension "shp" or "geojson".
        :param target_crs: the target coordinate reference system (CRS) to which the buildings will be transformed.
            * 2263: NAD83 / New York Long Island (ftUS)
            * 4326: WGS 84 - longitude/latitude
        """
        self.target_crs = 2263
        self.buildings_gdf = self.get_buildings_gdf(file_path).compute()
        self.ny_points = []
        # self.buildings_gdf = self.buildings_gdf
        # print(self.buildings_gdf)
        # Transform a Point object from CRS 4326 to CRS 2263
        self.transformer_from_4326_to_target = self.get_transformer_from_4326_to_target()
        # Transform a Point object from CRS 2263 to CRS 4326
        self.transformer_from_target_to_4326 = self.get_transformer_from_target_to_4326()

    def get_buildings_gdf(self, file_path):
        """
        Optimized function to read the building data and transform to the target CRS using pyogrio.
        """
        # Use pyogrio to read the file
        data = pyogrio.read_dataframe(file_path)

        # Convert to a GeoDataFrame
        gdf = gpd.GeoDataFrame(data, geometry='geometry')

        # Convert to Dask GeoDataFrame
        dask_gdf = dgpd.from_geopandas(gdf, npartitions=1)

        # Transform the CRS
        return dask_gdf.to_crs(self.target_crs)

    def get_transformer_from_4326_to_target(self):
        return pyproj.Transformer.from_crs(
            4326, self.target_crs, always_xy=True
        )

    def get_transformer_from_target_to_4326(self):
        return pyproj.Transformer.from_crs(
            self.target_crs, 4326, always_xy=True
        )

    def transform_(self, lat, lng):
        """
        This function transforms a point from CRS 4326 to CRS 2263
        :param lat: latitude
        :param lng: longitude
        """
        return geom.Point(self.transformer_from_4326_to_target.transform(float(lng), float(lat)))

    def back_transform_(self, point_object):
        """
        This function transforms a point from CRS 2263 to CRS 4326
        :param point_object: a Point object from shapely.geometry.Point class.
        """
        return self.transformer_from_target_to_4326.transform(point_object.x, point_object.y)

    def generate_ray(self, point, heading, distance=500):
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

    def get_ny_point_from_global_point(self, point: Point) -> Point:
        lat, lng = point.y, point.x
        transformer = pyproj.Transformer.from_crs(4326, 2263, always_xy=True)
        x, y = transformer.transform(lng, lat)
        return Point(x, y)

    def get_global_point_from_ny_point(self, point: Point) -> Point:
        x, y = point.x, point.y
        transformer = pyproj.Transformer.from_crs(2263, 4326, always_xy=True)
        lng, lat = transformer.transform(x, y)
        return Point(lng, lat)

    def get_nearest_intersection(self, global_point: Point, heading, distance=500) -> Point:
        """
        This function finds the nearest intersection point of a ray from a point of interest with the nearest building.
        :param lat: latitude
        :param lng: longitude
        :param heading: the heading in degrees.
        :param distance: the distance in meter to be used to generate the ray from the point of interest.
        """
        # Initialize variables to store nearest intersection point and distance
        self.nearest_distance = float("inf")
        self.nearest_intersection_point = geom.Point(0, 0)
        ny_coordinates_point = self.get_ny_point_from_global_point(
            global_point)
        self.ray = self.generate_ray(ny_coordinates_point, heading)

        self.intersected_buildings = self.get_intersected_buildings(
            self.buildings_gdf, self.ray)

        for _, row in self.intersected_buildings.iterrows():
            building = row.geometry
            intersections = self.ray.intersection(building)

            if intersections:
                # Handle MultiLineString and LineString uniformly
                if intersections.geom_type == "MultiLineString":
                    intersection_points = [
                        Point(coord) for line in intersections.geoms for coord in line.coords]
                elif intersections.geom_type == "LineString":
                    intersection_points = [Point(coord)
                                           for coord in intersections.coords]
                else:
                    continue  # Skip unsupported geometry types

                # Find the nearest intersection point
                for intersection_point in intersection_points:
                    distance = intersection_point.distance(
                        ny_coordinates_point)
                    if distance < self.nearest_distance:
                        self.nearest_distance = distance
                        self.nearest_intersection_point = intersection_point

        if self.nearest_intersection_point != Point(0, 0):
            # self.ny_points.append(self.nearest_intersection_point)
            # print(self.nearest_intersection_point)
            return self.get_global_point_from_ny_point(self.nearest_intersection_point)
            # return self.get_ny_point_from_global_point(self.nearest_intersection_point)
        # Convert the nearest intersection point to latitude and longitude
        return self.nearest_intersection_point

    def get_intersected_buildings(self, buildings_gdf: gpd.GeoDataFrame, ray: gpd.GeoSeries) -> gpd.GeoDataFrame:
        """
        Function to filter buildings that intersect with the given ray.

        Parameters:
        - buildings_gdf (gpd.GeoDataFrame): The GeoDataFrame containing the buildings.
        - ray (gpd.GeoSeries): The geometry of the ray to check intersections.

        Returns:
        - gpd.GeoDataFrame: A GeoDataFrame containing the buildings that intersect with the ray.
        """
        sindex = buildings_gdf.sindex
        # print(187, buildings_gdf, sindex)
        intersected_buildings = buildings_gdf.iloc[
            [idx for idx in sindex.intersection(
                ray.bounds) if buildings_gdf.iloc[idx].geometry.intersects(ray)]
        ]

        return intersected_buildings

        # Reset index to ensure clean indexing

    def plot_map(self, point_buffer=10):
        print(self.nearest_intersection_point)
        nearest_intersection_2263 = self.get_ny_point_from_global_point(
            self.nearest_intersection_point)
        print(nearest_intersection_2263)
        ray_gdf = gpd.GeoDataFrame(dict(address="Test_ray", bbl=1, geometry=[
                                   self.ray]), crs=self.target_crs)
        intersection_point_df = gpd.GeoDataFrame(dict(address="Test_point", bbl=2, geometry=[
                                                 nearest_intersection_2263.buffer(point_buffer)]), crs=self.target_crs)
        # print("2263",self.nearest_intersection, nearest_intersection_2263)
        # print("intersection_point_df", intersection_point_df)
        m = self.buildings_gdf.explore()

        ray_gdf.explore(m=m, color='red')
        print(intersection_point_df)
        intersection_point_df.explore(m=m, color='yellow')
        folium.LayerControl().add_to(m)
        m.save("map.html")

        return m

    def plot_points_difference(self, original_points, updated_points: list[Point]):
        point_buffer = 10
        # Transform updated points to the target CRS
        updated_points = [
            point for point in updated_points if isinstance(point, Point)]

        # ny_points = [self.get_ny_point_from_global_point(
        #     point)for point in updated_points]
        ny_points = updated_points
        ny_points_gdf = gpd.GeoDataFrame(
            dict(
                # Same address for all points
                address=["Test_point"] * len(ny_points),
                # Same BBL for all points
                bbl=[2] * len(ny_points),
                geometry=ny_points                       # Geometry column with points
            ),
            crs="EPSG:4326"  # Coordinate reference system
        )
        m = self.buildings_gdf.explore()
        ny_points_gdf.explore(m=m, color="black", marker_type="circle", marker_kwds={"radius": 5})
        m.save("updated points.html")


# FILE_PATH = "./pluto (1).geojson"
# point = (40.7667734, -73.9808934)
# heading = 45

# myCalculator = GeolocationCalculator(FILE_PATH)
# result = myCalculator.get_nearest_intersection(point[0], point[1], heading)
# nearest_intersection = result["intersection"]
# myCalculator.plot_map()
# print("ABOVE is from single point")
# print(nearest_intersection)

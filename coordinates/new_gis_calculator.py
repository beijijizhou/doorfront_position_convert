

import pyproj
import folium
import numpy as np
import geopandas as gpd
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
        # self.buildings_gdf = self.buildings_gdf
        # print(self.buildings_gdf)
        # Transform a Point object from CRS 4326 to CRS 2263
        self.transformer_ = self.get_transformer()
        # Transform a Point object from CRS 2263 to CRS 4326
        self.back_transformer_ = self.get_back_transformer_()

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

    def get_transformer(self):
        return pyproj.Transformer.from_crs(
            4326, self.target_crs, always_xy=True
        )

    def get_back_transformer_(self):
        return pyproj.Transformer.from_crs(
            self.target_crs, 4326, always_xy=True
        )

    def transform_(self, lat, lng):
        """
        This function transforms a point from CRS 4326 to CRS 2263
        :param lat: latitude
        :param lng: longitude
        """
        return geom.Point(self.transformer_.transform(float(lng), float(lat)))

    def back_transform_(self, point_object):
        """
        This function transforms a point from CRS 2263 to CRS 4326
        :param point_object: a Point object from shapely.geometry.Point class.
        """
        return self.back_transformer_.transform(point_object.x, point_object.y)

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

    def generate_bbox(geom_obj):
        """
        This function generates a bounding box from a geometry object.
        :param geom_obj: a geometry object from shapely.geometry class.
        """
        bounds = geom_obj.bounds
        # The order is left_top, left_bottom, right_bottom, right_top
        bbox = geom.Polygon(
            [
                (bounds[0], bounds[1]),
                (bounds[0], bounds[3]),
                (bounds[2], bounds[3]),
                (bounds[2], bounds[1]),
            ]
        )
        return bbox

    def get_nearest_intersection(self, lat, lng, heading, distance=500):
        """
        This function finds the nearest intersection point of a ray from a point of interest with the nearest building.
        :param lat: latitude
        :param lng: longitude
        :param heading: the heading in degrees.
        :param distance: the distance in meter to be used to generate the ray from the point of interest.
        """
        # Initialize variables to store nearest intersection point and distance
        self.nearest_distance = float("inf")
        self.nearest_intersection = None

        # Initialize variables to store the point of interest and the ray
        geo_point = self.transform_(lat, lng)
        self.ray = self.generate_ray(geo_point, heading)
        # print(self.buildings_gdf.intersects(self.ray))
        # print(self.buildings_gdf[self.buildings_gdf.intersects(self.ray)], self.ray)
        # Find all buildings that intersect with the ray
        self.candidate_buildings = self.get_candidate_buildings(
            self.buildings_gdf, self.ray)
        # print(self.candidate_buildings)
        # print(self.candidate_buildings, filtered_gdf)
        # print("115", self.candidate_buildings)
        for index, row in self.candidate_buildings.iterrows():
            building = row.geometry
            intersections = self.ray.intersection(building)
            if intersections:
                if intersections.geom_type == "MultiLineString":
                    for line in intersections.geoms:
                        for point in line.coords:
                            # Convert the coordinates to a Point object
                            intersection_point = geom.Point(point)
                            distance = intersection_point.distance(geo_point)
                            if distance < self.nearest_distance:
                                self.nearest_distance = distance
                                self.nearest_intersection = intersection_point
                elif intersections.geom_type == "LineString":
                    for intersection in intersections.coords:
                        intersection_point = geom.Point(intersection)
                        distance = intersection_point.distance(geo_point)
                        if distance < self.nearest_distance:
                            self.nearest_distance = distance
                            self.nearest_intersection = intersection_point

        # Convert the nearest intersection point to latitude and longitude
        if self.nearest_intersection:
            self.nearest_intersection = self.back_transform_(
                self.nearest_intersection)
        return {"distance": self.nearest_distance, "intersection": self.nearest_intersection}

    def get_candidate_buildings(self, buildings_gdf: gpd.GeoDataFrame, ray: gpd.GeoSeries) -> gpd.GeoDataFrame:
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
        nearest_intersection_2263 = self.transform_(
            self.nearest_intersection[1], self.nearest_intersection[0])

        ray_gdf = gpd.GeoDataFrame(dict(address="Test_ray", bbl=1, geometry=[
                                   self.ray]), crs=self.target_crs)
        intersection_point_df = gpd.GeoDataFrame(dict(address="Test_point", bbl=2, geometry=[
                                                 nearest_intersection_2263.buffer(point_buffer)]), crs=self.target_crs)

        m = self.buildings_gdf.explore()

        ray_gdf.explore(m=m, color='red')
        intersection_point_df.explore(m=m, color='yellow')
        folium.LayerControl().add_to(m)
        m.save("map.html")

        return m

# FILE_PATH = "./pluto (1).geojson"
# point = (40.7667734,-73.9808934)
# heading = 45

# myCalculator = GeolocationCalculator(FILE_PATH)
# result = myCalculator.get_nearest_intersection(point[0], point[1], heading)
# nearest_intersection = result["intersection"]
# print(nearest_intersection)

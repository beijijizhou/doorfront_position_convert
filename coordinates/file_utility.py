import csv
import time
import os
import shapely.geometry as geom
from shapely import Point
import pandas as pd
import folium
import geopandas as gpd

def time_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        result = func(*args, **kwargs)  # Call the original function
        end_time = time.time()  # End time
        print(
            f"Execution time for {func.__name__}: {end_time - start_time:.4f} seconds")
        return result
    return wrapper

output_file = "corrected_doorfront_data.csv"
@time_function
def saveFileInCSV(data):
    
    with open(output_file, "w", newline="") as file:
        # Use keys from the first item as field names
        fieldnames = data[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Updated data saved to {output_file}")


import pandas as pd
import folium

import geopandas as gpd
import folium
from shapely.geometry import Point, LineString


  
  


# Example usage
# plot_lat_long_on_map('corrected_doorfront_data.csv')


# Example usage
# plot_lat_long_on_map('corrected_doorfront_data.csv')

    
@time_function
def read_doorfront_data(file_path, geo_calculator):
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
            # data = json.load(file)
        length = 100

        original_points = [(0, 0)] * length
        updated_points = [(0, 0)] * length
        empty_building = 0
        intersected_building = 0
        for idx, item in enumerate(data[:length]):
            latitude = item.get("latitude")
            longitude = item.get("longitude")
            markerpov_heading = item.get("markerpov_heading")
            # print(latitude,longitude,markerpov_heading)
            # Add to the result if all fields are present
            if latitude and longitude and markerpov_heading:
                # print(latitude, longitude, markerpov_heading)
                point = Point(longitude, latitude)
                heading = markerpov_heading
                nearest_intersection_point = geo_calculator.get_nearest_intersection(
                    point, heading)

                if nearest_intersection_point != Point(0, 0):
                    # print(nearest_intersection["intersection"])
                    intersected_building += 1
                    # print(nearest_intersection["intersection"])
                    point = nearest_intersection_point
                    item["latitude"], item["longitude"] = nearest_intersection_point.y, nearest_intersection_point.x
                    # item["longitude"], item["latitude"] = nearest_intersection["intersection"]
                    item["markerpov_heading"] = 0
                    original_points[idx] = point
                    updated_points[idx] = nearest_intersection_point
                else:
                    empty_building += 1
        saveFileInCSV(data)
        geo_calculator.plot_points_difference(original_points, updated_points)
        print("total data point entry is ", length)
        print("found intersection is ", intersected_building)
        print("empty intersection is ", empty_building)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []

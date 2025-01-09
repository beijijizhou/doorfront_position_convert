from new_gis_calculator import GeolocationCalculator
import json
import csv
import time
import os
def time_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        result = func(*args, **kwargs)  # Call the original function
        end_time = time.time()  # End time
        print(f"Execution time for {func.__name__}: {end_time - start_time:.4f} seconds")
        return result
    return wrapper
@time_function
def read_doorfront_data(file_path, geo_calculator):
    """
    Reads the JSON data from the given file path and extracts
    latitude, longitude, and markerpov_heading for each entry.

    :param file_path: Path to the JSON file containing doorfront data.
    :return: A list of dictionaries containing the extracted data.
    """
    try:
        # Read the JSON file
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
            # data = json.load(file)
        # print(data[:10])
        # Extract relevant fields
        total_data = 10000
        empty_building = 0
        intersected_building = 0
        for item in data[:total_data]:
            latitude = item.get("latitude")
            longitude = item.get("longitude")
            markerpov_heading = item.get("markerpov_heading")
            # print(latitude,longitude,markerpov_heading)
            # Add to the result if all fields are present
            if latitude and longitude and markerpov_heading:
                # print(latitude, longitude, markerpov_heading)
                point = (latitude, longitude)
                heading = markerpov_heading
                nearest_intersection = geo_calculator.get_nearest_intersection(
                    point[0], point[1], heading)
                # print(nearest_intersection)
                # print(nearest_intersection)
                if nearest_intersection["intersection"] is not None:
                    # print(nearest_intersection["intersection"])
                    intersected_building += 1
                    # Replace latitude and longitude with the nearest intersection values
                else:
                    # Handle case where no intersection is found (optional)
                    # print(f"No intersection found for point: {point}")
                    empty_building += 1
        print("total data point entry is ", total_data)
        print("found intersection is ", intersected_building)        
        print("empty intersection is ", empty_building)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: File {file_path} is not a valid JSON.")
        return []


def main():
    # Path to the GeoJSON file
    pluto_file_path = "./pluto (1).geojson"
    # doorfront_data_file_path = "./doorfront_data.json"
    doorfront_data_file_path = "./doorfront.csv"
    if os.path.exists(pluto_file_path):
        print(f"File exists: {pluto_file_path}")
    else:
        raise FileNotFoundError(f"File not found: {pluto_file_path}")
    geo_calculator = GeolocationCalculator(pluto_file_path)
    
    # End timer for GeolocationCalculator initialization
    

    # Start timer for read_doorfront_data function

    # Call read_doorfront_data function
    # read_doorfront_data(doorfront_data_file_path, geo_calculator)

   

    # result = geo_calculator.get_nearest_intersection(point[0], point[1], heading)
    # nearest_intersection_lat_lon = result["intersection"]
    # print(nearest_intersection_lat_lon)


if __name__ == "__main__":
    main()

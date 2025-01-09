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
def saveFileInCSV(data):
    output_file = "corrected_doorfront_data.csv"
    with open(output_file, "w", newline="") as file:
        fieldnames = data[0].keys()  # Use keys from the first item as field names
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Updated data saved to {output_file}")

@time_function  
def read_doorfront_data(file_path, geo_calculator):
    try:
        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
            # data = json.load(file)
        total_data = len(data)
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
                    item["longitude"], item["latitude"] = nearest_intersection["intersection"]
                    item["markerpov_heading"] = 0
                else:
                    empty_building += 1
        saveFileInCSV(data)
        print("total data point entry is ", total_data)
        print("found intersection is ", intersected_building)        
        print("empty intersection is ", empty_building)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []
    
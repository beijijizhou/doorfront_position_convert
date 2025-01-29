

from new_gis_calculator import GeolocationCalculator
from file_utility import read_doorfront_data
import os
def main():
    # Path to the GeoJSON file
    file_path = "./full_nyc_buildings.geojson"
    # file_path = "./pluto (1).geojson"
    if os.path.exists(file_path):
        print(f"File exists: {file_path}")
    else:
        raise FileNotFoundError(f"File not found: {file_path}")
    # doorfront_data_file_path = "./doorfront_data.json"
    doorfront_data_file_path = "./doorfront.csv"
    geo_calculator = GeolocationCalculator(file_path)
    read_doorfront_data(doorfront_data_file_path, geo_calculator)
if __name__ == "__main__":
    main()
    


from new_gis_calculator import GeolocationCalculator
from file_utility import read_doorfront_data
import os
def main():
    # Path to the GeoJSON file
    pluto_file_path = "./full_nyc_buildings.geojson"
    # doorfront_data_file_path = "./doorfront_data.json"
    doorfront_data_file_path = "./doorfront.csv"
    geo_calculator = GeolocationCalculator(pluto_file_path)
    read_doorfront_data(doorfront_data_file_path, geo_calculator)
    
if __name__ == "__main__":
    main()

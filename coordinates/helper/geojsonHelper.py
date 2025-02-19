import folium
import pandas as pd

from helper.fileHelper import read_data


def plot_geojson_address(row: dict, map_plot: folium.Map) -> None:
    lat, lon = float(row['latitude']), float(row['longitude'])
    folium.Marker(
        location=[lat, lon],
        popup="Address",
        icon=folium.Icon(color='red')
    ).add_to(map_plot)


def plot_all_geojson_address(map_plot: folium.Map) -> None:
    csv_file_path = "geojson.csv"
    data = read_data(csv_file_path)
    for index, row in data.iterrows():
        plot_geojson_address(row, map_plot)

    return

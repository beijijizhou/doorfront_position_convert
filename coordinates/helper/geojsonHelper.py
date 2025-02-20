import folium
import pandas as pd
import shapely

from helper.fileHelper import read_data


def plot_geojson_address(row: dict, map_plot: folium.Map) -> None:
    lat, lon = float(row['latitude']), float(row['longitude'])
    folium.Marker(
        location=[lat, lon],
        tooltip=f"Geojson Address: {row['address']}",
        icon=folium.Icon(color='red')
    ).add_to(map_plot)


def plot_geometry(row: dict, map_plot: folium.Map):
    if 'geometry' in row:
        polygon = shapely.wkt.loads(row['geometry'])
        if polygon.geom_type == "Polygon":
            folium.Polygon(
                # Swap (lon, lat) to (lat, lon)
                locations=[(point[1], point[0])
                           for point in polygon.exterior.coords],
                color="red",
                fill=True,
                fill_opacity=0.4
            ).add_to(map_plot)


def plot_all_geojson_address(map_plot: folium.Map) -> None:
    csv_file_path = "geojson.csv"
    data = read_data(csv_file_path)
    for index, row in data.iterrows():

        plot_geojson_address(row, map_plot)
        plot_geometry(row, map_plot)
    return

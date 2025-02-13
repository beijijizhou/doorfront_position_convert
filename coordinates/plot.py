import sys

from geopandas import GeoDataFrame
from typing import Dict, List, Tuple
from typing import Optional, Tuple
from dask_geopandas import GeoDataFrame
import pandas as pd
import folium
from shapely import MultiPolygon, Point
import random
from geopandas import GeoSeries
from helper.fileHelper import get_random_sample
from helper.nominatimHelper import get_nominatim_address
from geojson_reader import get_buildings_gdf
from helper.geoHelper import generate_ray, get_intersection

gdf: GeoDataFrame = None
map_plot: folium.Map = None


def create_custom_popup(row: pd.Series):
    """
    Create a custom HTML layout for the popup with attribute names on the left and values on the right.

    Parameters:
        row (pd.Series): A row of the DataFrame containing the attributes and values.

    Returns:
        str: The HTML string for the popup.
    """
    popup_html = "<b>Attributes:</b><br>"
    for col, value in row.items():
        popup_html += f"<b>{col}:</b> {value}<br>"
    return popup_html


def get_geojson_address(row: pd.Series) -> pd.Series:
    row_copy = row.copy()
    lat_lon = (row_copy['latitude'], row_copy['longitude'])
    heading = row_copy['markerpov_heading']

    ray = get_and_plot_ray_on_map(lat_lon, heading)
    intersected_building, intersection_point = get_and_plot_intersection(
        ray, gdf)
    if intersection_point:
        row_copy['latitude'] = intersection_point.y
        row_copy['longitude'] = intersection_point.x
        row_copy['address'] = intersected_building["address"]
        row_copy["geometry"] = intersected_building["geometry"]
    return row_copy





def plot_nominatim_data(nominatim_data):
    lat, lon, bounding_box = nominatim_data['nominatim_latitude'], nominatim_data[
        'nominatim_longitude'], nominatim_data['boundingbox']

    # Plot Nominatim data with a different color icon
    folium.Marker(
        location=[lat, lon],
        popup="Nominatim Data",
        icon=folium.Icon(color='blue'),  # Different color for Nominatim data
        tooltip="Nominatim Data"  # Tooltip to indicate Nominatim data
    ).add_to(map_plot)
    print()
    # Plot bounding box using Polygon (convert bounding box string to coordinates)
    


def get_address(data: pd.DataFrame, color: str) -> List[pd.Series]:
    global map_plot
    # Dictionary to keep track of lat, lon counts
    lat_lon_count: Dict[Tuple[float, float], int] = {}
    success: int = 0
    fail: int = 0
    geojson_address_arr: List[pd.Series] = []
    nominatim_address_arr: List[dict] = []
    for _, row in data.iterrows():
        try:
            # Tuple of lat and lon
            lat_lon: Tuple[float, float] = (row['latitude'], row['longitude'])
            # Increment the count for this lat, lon pair
            lat_lon_count[lat_lon] = lat_lon_count.get(lat_lon, 0) + 1
            geojson_addres = get_geojson_address(row)
            nominatim_address = get_nominatim_address(row)
            plot_nominatim_data(nominatim_address)
            # nominatim_address = get_nominatim_address(row)
            # print(type(nominatim_address))
            # Add the marker to the map
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=create_custom_popup(row),
                icon=folium.Icon(color=color),
                draggable=True,
                tooltip="Original Doorfront Data",  # Tooltip with title

            ).add_to(map_plot)

            success += 1
        except Exception as e:
            fail += 1
            print(f"Error processing row {row.name}: {e}")

    print("success", success)
    print("fail", fail)


def plot_intersection_point(intersection_point):
    """Plot an intersection point with a popup link to open in Google Maps."""
    lat, lon = intersection_point.y, intersection_point.x
    google_maps_url = f"https://www.google.com/maps?q={lat},{lon}"

    popup_content = f"""
    <b>Coordinates:</b><br>
    Lat: {lat}, Lon: {lon}<br>
    <a href="{google_maps_url}" target="_blank">Open in Google Maps</a>
    """

    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(popup_content, max_width=250),
        icon=folium.Icon(color='blue')
    ).add_to(map_plot)


def get_and_plot_intersection(ray: GeoSeries, gdf: GeoDataFrame) -> Tuple[Optional[GeoSeries], Optional[Point]]:
    intersected_building, intersection_point = get_intersection(ray, gdf)
    if isinstance(intersected_building['geometry'], MultiPolygon):
        intersected_building['geometry'] = intersected_building['geometry'].geoms[0]
    # building_geojson = gdf.GeoSeries(intersected_building['geometry']).__geo_interface__
    folium.GeoJson(
        intersected_building['geometry'],  # The geometry of the building
        style_function=lambda x: {
            'fillColor': 'blue',  # Fill color of the polygon
            'color': 'blue',      # Border color of the polygon
            'weight': 2,          # Border thickness
            'fillOpacity': 0.4    # Fill opacity
        },
        tooltip=f"Address: {intersected_building['address']}<br>BBL: {
            intersected_building['bbl']}<br>Borough: {intersected_building['borough']}"
    ).add_to(map_plot)

    plot_intersection_point(intersection_point)
    return intersected_building, intersection_point


def get_and_plot_ray_on_map(lat_lon, heading):
    point = Point(lat_lon[1], lat_lon[0])  # Create a Point object (lon, lat)
    ray = generate_ray(point, heading)  # Generate the ray
    ray_geometry = ray.geometry[0]
    folium.GeoJson(
        ray_geometry,
        style_function=lambda x: {'color': 'black',
                                  'weight': 3}  # Customize the line style
    ).add_to(map_plot)

    return ray
    # Extract coordinates from the LineString object


def printDuplicate(lat_lon_count):
    # Print the duplicates (lat, lon pairs that appear more than once)
    print("Duplicate latitudes and longitudes (count > 1):")
    total_dup = 0
    for lat_lon, count in lat_lon_count.items():
        if count > 1:
            print(f"Latitude: {lat_lon[0]}, Longitude: {
                  lat_lon[1]} - Count: {count}")
            total_dup += 1
    print(f"The total duplicate is {total_dup}")





def create_map_with_markers(old_data_path):
    global gdf, map_plot
    # Load the data from both files
    manhattan_file_path = "./manhattan.geojson"
    old_data = pd.read_csv(old_data_path)
    old_data = get_random_sample(old_data)
    # Create a map centered around the average location of both datasets
    map_center = [
        (old_data['latitude'].mean() + old_data['latitude'].mean()) / 2,
        (old_data['longitude'].mean() + old_data['longitude'].mean()) / 2
    ]
    # map_plot = folium.Map(location=map_center,
    #                       zoom_start=12, tiles='OpenStreetMap')
    map_plot = m = folium.Map(
        location=map_center,
        zoom_start=15,
        tiles='https://mt1.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
        attr='© Google Maps'
    )
    gdf = get_buildings_gdf(manhattan_file_path)
    gdf = gdf.to_crs('EPSG:4326')

    get_address(old_data, color='red')
    # save_corrected_data(corrected_data)
    return map_plot

# Example usage
# create_map_with_markers('corrected_doorfront_data.csv', 'doorfront.csv')

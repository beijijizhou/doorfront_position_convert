import pandas as pd
import folium
from shapely import Point

from geojson_reader import get_buildings_gdf
from help import generate_ray, get_intersection

gdf = None


def create_custom_popup(row):
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


def add_markers_to_map(map_plot, data, color):
    # Dictionary to keep track of lat, lon counts
    lat_lon_count = {}
    success = fail = 0
    for _, row in data.iterrows():
        try:
            # Tuple of lat and lon
            lat_lon = (row['latitude'], row['longitude'])
            heading = row['markerpov_heading']

            # integrate_with_lat_lon(lat_lon)
            # Increment the count for this lat, lon pair
            if lat_lon in lat_lon_count:
                lat_lon_count[lat_lon] += 1
            else:
                lat_lon_count[lat_lon] = 1
            plot_ray_on_map(lat_lon, heading, map_plot)
            # Add the marker to the map
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=create_custom_popup(row),
                icon=folium.Icon(color=color),
                draggable=True,
            ).add_to(map_plot)
            success += 1
        except Exception as e:
            fail += 1
            print(f"Error processing row {row.name}: {e}")
    print("sucess", success)
    print("fail", fail)
    # printDuplicate(lat_lon_count)


def plot_intersection_point(intersection_point,  map_plot):
    """
    Plot the closest intersection point on the map and print the results.

    Args:
        intersection_point (shapely.geometry.Point): The closest intersection point.
        intersection_count (int): The total number of intersections.
        map_plot (folium.Map): The Folium map to which the intersection will be added.

    Returns:
        folium.Map: The updated Folium map.
    """

    # print(f"Closest intersection point: {intersection_point}")
    folium.Marker(
        location=[intersection_point.y, intersection_point.x],
        icon=folium.Icon(color='blue')
    ).add_to(map_plot)

    return map_plot


def get_and_plot_intersection(ray, gdf, map_plot):
    """
    Find the closest intersection point, count intersections, and plot the result on the map.

    Args:
        ray (shapely.geometry.LineString): The ray (LineString) to check for intersections.
        gdf (geopandas.GeoDataFrame): The GeoDataFrame containing building geometries.
        map_plot (folium.Map): The Folium map to which the intersection will be added.

    Returns:
        tuple: A tuple containing:
            - shapely.geometry.Point: The closest intersection point, or None if no intersection is found.
            - int: The total number of intersections.
            - folium.Map: The updated Folium map.
    """
    # Get the intersection point and count
    intersection_point = get_intersection(ray, gdf)
    print(intersection_point)
    # Plot the intersection
    map_plot = plot_intersection_point(
        intersection_point, map_plot)


def plot_ray_on_map(lat_lon, heading, map_plot):
    global gdf
    point = Point(lat_lon[1], lat_lon[0])  # Create a Point object (lon, lat)
    ray = generate_ray(point, heading)  # Generate the ray
    # folium.GeoJson(ray.geometry[0], style_function=lambda x: {'color': 'red'}).add_to(map_plot)
    ray_geometry = ray.geometry[0]
    folium.GeoJson(
        ray_geometry,
        style_function=lambda x: {'color': 'black',
                                  'weight': 3}  # Customize the line style
    ).add_to(map_plot)
    get_and_plot_intersection(ray, gdf, map_plot)

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


def create_map_with_markers(new_data_path, old_data_path):
    global gdf
    # Load the data from both files
    geojson_file_path = "./full_nyc_buildings.geojson"
    manhattan_file_path = "./manhattan.geojson"

    max_row = 1000
    new_data = pd.read_csv(new_data_path)[:max_row]
    old_data = pd.read_csv(old_data_path)[:max_row]
    # Create a map centered around the average location of both datasets
    map_center = [
        (new_data['latitude'].mean() + old_data['latitude'].mean()) / 2,
        (new_data['longitude'].mean() + old_data['longitude'].mean()) / 2
    ]
    map_plot = folium.Map(location=map_center, zoom_start=12)
    gdf = get_buildings_gdf(manhattan_file_path)
    gdf = gdf.to_crs('EPSG:4326')

    # folium.GeoJson(
    #     gdf,  # Your GeoDataFrame
    #     name="Buildings",  # Layer name
    #     tooltip=folium.GeoJsonTooltip(
    #         fields=["address", "bbl"],  # Use the actual column names in your GeoDataFrame
    #         aliases=["Address", "BBL"]  # Customize the tooltip labels
    #     ),
    #     style_function=lambda x: {'fillColor': 'blue', 'color': 'black', 'weight': 1}  # Style the polygons
    # ).add_to(map_plot)
    # Add markers for both datasets
    # add_markers_to_map(map_plot, new_data, color='blue')  # First dataset (blue markers)
    # Second dataset (red markers)
    add_markers_to_map(map_plot, old_data, color='red')
    # Save the map to an HTML file

    return map_plot

# Example usage
# create_map_with_markers('corrected_doorfront_data.csv', 'doorfront.csv')

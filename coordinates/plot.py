import pandas as pd
import folium
from shapely import Point

from help import generate_ray


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

        except Exception as e:
            print(f"Error processing row {row.name}: {e}")
    # printDuplicate(lat_lon_count)


def plot_ray_on_map(lat_lon, heading, map_plot):
    point = Point(lat_lon[1], lat_lon[0])  # Create a Point object (lon, lat)
    ray = generate_ray(point, heading)  # Generate the ray

    # Extract coordinates from the LineString object
    ray_coords = [(coord[1], coord[0])
                  for coord in ray.coords]  # Convert (lon, lat) to (lat, lon)
    # print(ray_coords)
    # Add the ray to the map as a PolyLine
    folium.PolyLine(
        locations=ray_coords,  # Pass the coordinates
        color='blue',          # Set the line color
        weight=2,              # Set the line weight
        opacity=0.8            # Set the line opacity
    ).add_to(map_plot)


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

    # Load the data from both files
    max_row = 400
    new_data = pd.read_csv(new_data_path)[:max_row]
    old_data = pd.read_csv(old_data_path)[:max_row]
    # Create a map centered around the average location of both datasets
    map_center = [
        (new_data['latitude'].mean() + old_data['latitude'].mean()) / 2,
        (new_data['longitude'].mean() + old_data['longitude'].mean()) / 2
    ]
    map_plot = folium.Map(location=map_center, zoom_start=12)
    # Add markers for both datasets
    # add_markers_to_map(map_plot, new_data, color='blue')  # First dataset (blue markers)
    # Second dataset (red markers)
    add_markers_to_map(map_plot, old_data, color='red')
    # Save the map to an HTML file
 

    return map_plot

# Example usage
# create_map_with_markers('corrected_doorfront_data.csv', 'doorfront.csv')

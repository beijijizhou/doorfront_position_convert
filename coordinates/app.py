import pandas as pd
import folium
from flask import Flask, render_template_string
app = Flask(__name__)

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
            lat_lon = (row['latitude'], row['longitude'])  # Tuple of lat and lon
            
            # Increment the count for this lat, lon pair
            if lat_lon in lat_lon_count:
                lat_lon_count[lat_lon] += 1
            else:
                lat_lon_count[lat_lon] = 1

            # Add the marker to the map
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=create_custom_popup(row),
                icon=folium.Icon(color=color),
                draggable=True,
            ).add_to(map_plot)
        
        except Exception as e:
            print(f"Error processing row {row.name}: {e}")

    # Print the duplicates (lat, lon pairs that appear more than once)
    print("Duplicate latitudes and longitudes (count > 1):")
    total_dup = 0
    for lat_lon, count in lat_lon_count.items():
        if count > 1:
            print(f"Latitude: {lat_lon[0]}, Longitude: {lat_lon[1]} - Count: {count}")
            total_dup += 1
    print(f"The total duplicate is {total_dup}")
# Example usage
# add_markers_to_map(map_plot, data, 'blue')


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
    add_markers_to_map(map_plot, new_data, color='blue')  # First dataset (blue markers)
    add_markers_to_map(map_plot, old_data, color='red')   # Second dataset (red markers)

    # Save the map to an HTML file
    output_file = 'map_plot.html'
    map_plot.save(output_file)
    print(f"Map has been saved to '{output_file}'")

    return map_plot._repr_html_()

# Example usage

@app.route('/')
def map_view():
    # Path to your CSV files
    new_data_path = 'corrected_doorfront_data.csv'
    old_data_path = 'doorfront.csv'
    
    # Generate the map HTML
    map_html = create_map_with_markers(new_data_path, old_data_path)
    
    # Render the map in the browser
    return render_template_string(map_html)

if __name__ == '__main__':
    app.run(debug=True)

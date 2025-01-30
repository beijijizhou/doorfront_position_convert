import pandas as pd
import folium
from folium.plugins import MarkerCluster
import requests
import os
from dotenv import load_dotenv
load_dotenv()

# Fetch the API key
API_KEY = os.getenv("GOOGLE_API_KEY")


def plot_coordinates_on_google_satellite(file_path, latitude_column='latitude', longitude_column='longitude', output_file="doorfront_coordinates_map.html"):
    """
    Plots coordinates from a CSV file on a Google Satellite layer and saves the map as an HTML file.

    Parameters:
        file_path (str): Path to the CSV file containing coordinates.
        latitude_column (str): Name of the column containing latitude values. Default is 'latitude'.
        longitude_column (str): Name of the column containing longitude values. Default is 'longitude'.
        output_file (str): Name of the output HTML file. Default is 'doorfront_coordinates_map.html'.
    """
    # Load the CSV file
    df = pd.read_csv(file_path)
    print(API_KEY)
    # Ensure the dataframe has the specified latitude and longitude columns
    if latitude_column not in df.columns or longitude_column not in df.columns:
        raise ValueError(f"CSV file must contain '{latitude_column}' and '{
                         longitude_column}' columns.")

    # Create a base map with Google Satellite layer
    m = folium.Map(
        location=[df[latitude_column].mean(), df[longitude_column].mean()],
        zoom_start=15,
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google Satellite'
    )

    # Add markers for each coordinate
    # marker_cluster = MarkerCluster().add_to(m)
    for index, row in df.iterrows():
        # Create a Google Street View URL
        lat, lon = row[latitude_column], row[longitude_column]
        # metadata_url = f"https://maps.googleapis.com/maps/api/streetview/metadata?location={lat},{lon}&key={API_KEY}"

        street_view_url = f"https://www.google.com/maps?layer=c&cbll={lat},{lon}"


        # Create a popup with a link to Street View
        popup_content = f"""
         <b>Address:</b><br>
        {row["address"]}<br>
        <b>Coordinates:</b><br>
        Lat: {row[latitude_column]}, Lon: {row[longitude_column]}<br>

        <a href="{street_view_url}" target="_blank">Open Street View</a>
        """
        popup = folium.Popup(popup_content, max_width=300)

        # Add the marker with the popup
        folium.Marker(
            location=[row[latitude_column], row[longitude_column]],
            popup=popup
        ).add_to(m)

    # Save the map to an HTML file
    return m

# Example usage
# csv_file = "/Users/hongzhonghu/Desktop/work/research/doorfront_position_convert/coordinates/corrected_doorfront_data.csv"
# plot_coordinates_on_google_satellite(csv_file)

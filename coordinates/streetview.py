import pandas as pd
import folium
import requests
import os
from folium.plugins import MarkerCluster
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch API key
API_KEY = os.getenv("GOOGLE_API_KEY")


def reverse_geocode(lat, lon):
    """Fetch address using Google Geocoding API"""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={API_KEY}"
    response = requests.get(url).json()

    if response["status"] == "OK":
        return response["results"][0]["formatted_address"]
    return "Unknown Address"


def get_all_addresses(df, latitude_column, longitude_column):
    """Fetch addresses in parallel using multithreading"""
    addresses = {}

    # Use ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_index = {executor.submit(reverse_geocode, row[latitude_column], row[longitude_column]): index for index, row in df.iterrows()}

        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                addresses[index] = future.result()
            except Exception as e:
                addresses[index] = "Error fetching address"

    return addresses


def plot_coordinates_on_google_satellite(file_path, latitude_column='latitude', longitude_column='longitude'):
    """
    Plots coordinates from a CSV file on a Google Satellite layer and saves the map as an HTML file.
    """
    # Load CSV file
    df = pd.read_csv(file_path)

    # Ensure the dataframe has the required columns
    if latitude_column not in df.columns or longitude_column not in df.columns:
        raise ValueError(f"CSV file must contain '{latitude_column}' and '{longitude_column}' columns.")

    # Fetch addresses using multithreading
    address_map = get_all_addresses(df, latitude_column, longitude_column)

    # Create a base map
    m = folium.Map(
        location=[df[latitude_column].mean(), df[longitude_column].mean()],
        zoom_start=15,
        tiles='https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google Satellite'
    )

    # Add markers
    for index, row in df.iterrows():
        lat, lon = row[latitude_column], row[longitude_column]

        # Generate Google Street View link
        street_view_url = f"https://www.google.com/maps?layer=c&cbll={lat},{lon}"

        # Get the reverse-geocoded address
        google_address = address_map.get(index, "Unknown Address")

        # Create a popup
        popup_content = f"""
         <b>Reverse Geocoded Address:</b><br>
        {google_address}<br>
         <b>Original CSV Address:</b><br>
        {row["address"]}<br>
        <b>Coordinates:</b><br>
        Lat: {lat}, Lon: {lon}<br>

        <a href="{street_view_url}" target="_blank">Open Street View</a>
        """
        popup = folium.Popup(popup_content, max_width=300)

        # Add marker
        folium.Marker(
            location=[lat, lon],
            popup=popup
        ).add_to(m)

    return m

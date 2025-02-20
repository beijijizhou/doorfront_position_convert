from typing import Optional, Tuple
import pandas as pd
import folium
import requests
import os
from folium.plugins import MarkerCluster
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time

from helper.fileHelper import read_data

# Load environment variables
load_dotenv()

# Fetch API key
API_KEY = os.getenv("GOOGLE_API_KEY")


def get_google_address(row: dict) -> Tuple[str, Optional[Tuple[float, float, float, float]]]:
    lat, lon = float(row['latitude']), float(row['longitude'])
    """Fetch address and bounding box using Google Geocoding API"""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={API_KEY}"
    try:
        response = requests.get(url).json()
        if response["status"] == "OK":
            result = response["results"][0]
            print(result)
            formatted_address = result["formatted_address"]
            geometry = result["geometry"]
            location = geometry.get("location", {})
            api_lat, api_lon = location.get(
                "lat", lat), location.get("lng", lon)
            # Extract bounding box (viewport)
            viewport = geometry.get("viewport", {})
            northeast = viewport.get("northeast", {})
            southwest = viewport.get("southwest", {})

            bounding_box = (
                southwest.get("lat", None),
                northeast.get("lat", None),
                southwest.get("lng", None),
                northeast.get("lng", None),
            )
            return api_lat, api_lon, formatted_address, bounding_box
    except Exception as e:
        print(f"Error fetching address for ({lat}, {lon}): {e}")

    return "Unknown Address", None


def get_all_addresses(df, latitude_column, longitude_column, batch_size=100):
    """
    Fetch addresses in parallel using multithreading, processing 100 rows at a time.

    :param df: Input DataFrame
    :param latitude_column: Name of latitude column
    :param longitude_column: Name of longitude column
    :param batch_size: Number of rows to process in one batch
    :return: DataFrame with reverse-geocoded addresses
    """
    df["google_address"] = "Processing..."
    df["api_lat"] = None  # Add column for API latitude
    df["api_lon"] = None  # Add column for API longitude
    df["bounding_box"] = None  # Add column for bounding box

    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        print(f"Processing batch {start} to {end}...")

        batch_df = df.iloc[start:end]
        addresses = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_index = {executor.submit(get_google_address, row): index
                               for index, row in batch_df.iterrows()}

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    if result:
                        # Unpack the result (api_lat, api_lon, address, bbox)
                        api_lat, api_lon, formatted_address, bounding_box = result
                        # Update the DataFrame with the fetched data
                        df.at[index, "google_address"] = formatted_address
                        df.at[index, "api_lat"] = api_lat
                        df.at[index, "api_lon"] = api_lon
                        df.at[index, "bounding_box"] = bounding_box
                    else:
                        df.at[index, "google_address"] = "Error fetching address"
                except Exception as e:
                    df.at[index, "google_address"] = "Error fetching address"

        # Small delay to avoid hitting API rate limits
        time.sleep(1)

    return df


def read_and_get_google_address(file_path):
    """
    Plots coordinates from a CSV file on a Google Satellite layer and saves the map as an HTML file.
    Also updates the CSV file with reverse-geocoded addresses.
    """
    # Load CSV file
    latitude_column = 'latitude'
    longitude_column = 'longitude'
    output_csv = "google_address.csv"
    df = pd.read_csv(file_path)[:100]

    # Ensure the dataframe has the required columns
    if latitude_column not in df.columns or longitude_column not in df.columns:
        raise ValueError(f"CSV file must contain '{latitude_column}' and '{
                         longitude_column}' columns.")

    # Fetch and save reverse-geocoded addresses in batches
    df = get_all_addresses(df, latitude_column,
                           longitude_column, batch_size=100)

    # Save the updated DataFrame to CSV
    df.to_csv(output_csv, index=False)
    print(f"Updated CSV saved: {output_csv}")


def plot_google_file(map_plot):
    # Read the CSV file
    file_path = "google_address.csv"
    df = read_data(file_path)
    for _, row in df.iterrows():
        google_lat = row['google_lat']
        google_lon = row['google_lon']
        bounding_box = row['bounding_box']

        if pd.notna(google_lat) and pd.notna(google_lon) and pd.notna(bounding_box):
            try:
                # Unpack the bounding box string (assuming it's in the format: "(min_lat, max_lat, min_lon, max_lon)")
                # This converts the string to a tuple
                bbox = eval(bounding_box)
                min_lat, max_lat, min_lon, max_lon = bbox

                # Create a marker with the google_lat, google_lon and address as popup
                folium.Marker(
                    location=[google_lat, google_lon],
                    # Tooltip with the address
                    tooltip=f"Google Address: {row['google_address']}",
                    icon=folium.Icon(color='black'),  # Marker color
                ).add_to(map_plot)

                # Add a rectangle for the bounding box
                # folium.Rectangle(
                #     bounds=[(min_lat, min_lon), (max_lat, max_lon)],
                #     color="black",  # Rectangle color
                #     fill=True,  # Fill the rectangle with color
                #     fill_color="black",  # Fill color
                #     fill_opacity=0.2  # Set opacity for the fill
                # ).add_to(map_plot)
                # folium.Marker(
                #     location=[min_lat, min_lon],
                #     popup="Bounding Box Point 1",
                #     icon=folium.Icon(color='purple'),
                #     tooltip="BB Point 1"
                # ).add_to(map_plot)

                # folium.Marker(
                #     location=[max_lat, max_lon],
                #     popup="Bounding Box Point 2",
                #     icon=folium.Icon(color='purple'),
                #     tooltip="BB Point 2"
                # ).add_to(map_plot)
            except Exception as e:
                print(f"Error plotting data for row {row['object_id']}: {e}")

    # Save the map as an HTML file
    return map_plot

# Call the function with your CSV file path

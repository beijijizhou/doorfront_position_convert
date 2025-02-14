from typing import Optional, Tuple
import pandas as pd
import folium
import requests
import os
from folium.plugins import MarkerCluster
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import time

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
            formatted_address = result["formatted_address"]
            geometry = result["geometry"]
            location = geometry.get("location", {})
            api_lat, api_lon = location.get("lat", lat), location.get("lng", lon)
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
            return api_lat, api_lon,formatted_address, bounding_box
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
    df["reverse_geocoded_address"] = "Processing..."

    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        print(f"Processing batch {start} to {end}...")

        batch_df = df.iloc[start:end]
        addresses = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_index = {executor.submit(get_google_address, row[latitude_column], row[longitude_column]): index
                               for index, row in batch_df.iterrows()}

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    addresses[index] = future.result()
                except Exception:
                    addresses[index] = "Error fetching address"

        # Update the DataFrame with the fetched addresses
        df.loc[start:end-1, "reverse_geocoded_address"] = df.loc[start:end -
                                                                 1].index.map(addresses)

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
    # df.to_csv(output_csv, index=False)
    # print(f"Updated CSV saved: {output_csv}")

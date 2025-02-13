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


def reverse_geocode(lat, lon):
    """Fetch address using Google Geocoding API"""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={
        lat},{lon}&key={API_KEY}"
    try:
        response = requests.get(url).json()
        if response["status"] == "OK":
            return response["results"][0]["formatted_address"]
    except Exception as e:
        print(f"Error fetching address for ({lat}, {lon}): {e}")

    return "Unknown Address"


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
            future_to_index = {executor.submit(reverse_geocode, row[latitude_column], row[longitude_column]): index
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


def get_google_address(file_path, latitude_column='latitude', longitude_column='longitude', output_csv="updated_coordinates.csv"):
    """
    Plots coordinates from a CSV file on a Google Satellite layer and saves the map as an HTML file.
    Also updates the CSV file with reverse-geocoded addresses.
    """
    # Load CSV file
    df = pd.read_csv(file_path)

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



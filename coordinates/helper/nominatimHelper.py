import folium
import requests
import json
import pandas as pd





def get_nominatim_address(row):
    lat, lon = float(row['latitude']), float(row['longitude'])
    nominatim_url = "http://localhost:8080/reverse"
    params = {"format": "json", "lat": lat, "lon": lon}

    try:
        response = requests.get(nominatim_url, params=params)
        if response.status_code == 200:
            data = response.json()
            nominatim_lat = float(data.get("lat", lat))
            nominatim_lon = float(data.get("lon", lon))
            address_components = data.get("address", {})
            house_number = address_components.get("house_number", "")
            road = address_components.get("road", "")

            if house_number and road:
                display_name = f"{house_number} {road}"
            else:
                display_name = "N/A"
            key_details = {
                "display_name": display_name,
                "nominatim_latitude": nominatim_lat,
                "nominatim_longitude": nominatim_lon,
                "boundingbox": data.get("boundingbox", []),
                "full_response": json.dumps(data)
            }
            return key_details
    except requests.RequestException as e:
        return {
            "display_name": str(e),
            "nominatim_latitude": None,
            "nominatim_longitude": None,
            "boundingbox": "N/A",
            "full_response": None
        }


def plot_nominatim_marker(map_plot: folium.Map, address: dict) -> None:
    lat = address['nominatim_latitude']
    lon = address['nominatim_longitude']
    display_name = address['display_name']
    boundingbox = address['boundingbox']

    # Add marker for the address
    folium.Marker(
        location=[lat, lon],
        popup=display_name,
        icon=folium.Icon(color='blue')
    ).add_to(map_plot)

    # Plot bounding box if available
    if boundingbox and len(boundingbox) == 4:
        north = float(boundingbox[0])
        south = float(boundingbox[1])
        east = float(boundingbox[2])
        west = float(boundingbox[3])
        folium.Rectangle(
            bounds=[[south, west], [north, east]],
            color='blue',
            fill=True,
            fill_opacity=0.2,
            weight=1
        ).add_to(map_plot)


def get_all_nominatim_address(data: pd.DataFrame, map_plot: folium.Map) -> None:
    for index, row in data.iterrows():
        address_details = get_nominatim_address(row)
        if address_details["nominatim_latitude"] is not None and address_details["nominatim_longitude"] is not None:
            # Pass the address object
            plot_nominatim_marker(map_plot, address_details)

    return

# Example usage:
# Create a map object

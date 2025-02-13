import pandas as pd
import requests
import json



def get_nominatim_address(row):
    
    lat, lon = float(row['latitude']), float(row['longitude'])
    nominatim_url = "http://localhost:8080/reverse"
    params = {"format": "json", "lat": lat, "lon": lon}

    try:
        response = requests.get(nominatim_url, params=params)
        if response.status_code == 200:
            data = response.json()

            # Extract additional lat/lon from Nominatim response
            nominatim_lat = float(data.get("lat", lat))  # Use original if missing
            nominatim_lon = float(data.get("lon", lon))

            key_details = {
                "display_name": data.get("display_name", "N/A"),
                "nominatim_latitude": nominatim_lat,
                "nominatim_longitude": nominatim_lon,
                "boundingbox": "|".join(data.get("boundingbox", [])),  # Convert to string
                "full_response": json.dumps(data)  # Store full JSON response if needed
            }
            print(key_details)
            return key_details
    except requests.RequestException as e:
        return {
            "display_name": str(e),
            "nominatim_latitude": None,
            "nominatim_longitude": None,
            "boundingbox": "N/A",
            "full_response": None
        }





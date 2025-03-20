import pymongo
import pandas as pd
import folium
from pprint import pprint  # For nicer output formatting
map_plot = None


def addMarker(lat, lon, address, color="red"):
    folium.CircleMarker(
        location=[lat, lon],
        radius=5,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.7,
        popup=f"{address} ({lat}, {lon})"  # Display address with lat and lon

    ).add_to(map_plot)


def connect_to_db(db_name="osm_ny", collection_name="buildings"):
    # Connect to MongoDB
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        collection = db[collection_name]
        print("Successfully connected to MongoDB")
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def ensure_2dsphere_index(collection):
    # Ensure 2dsphere index on 'centroid' field
    try:
        indexes = collection.index_information()
        if not any("centroid_2dsphere" in idx for idx in indexes):
            print("Creating 2dsphere index on 'centroid'...")
            collection.create_index([("centroid", "2dsphere")])
            print("2dsphere index created successfully")
        else:
            print("2dsphere index already exists")
    except Exception as e:
        print(f"Error managing 2dsphere index: {e}")


def query_closest_point(collection, lon, lat):
    # Query the closest point using $near
    try:
        query = {
            "centroid": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]  # [longitude, latitude]
                    },
                    "$maxDistance": 50  # Limit to 1000 meters
                }
            }
        }
        result = collection.find_one(query)
        if result:
            # print(f"\nFound closest document for ({lon}, {lat}):")
            # pprint(result)
            return result
        else:
            print(f"No document found near ({lon}, {lat}) within 1000 meters")
            return None
    except Exception as e:
        print(f"Error querying point ({lon}, {lat}): {e}")
        return None


def query_from_csv(local_map_plot, csv_path="geojson.csv", num_points=10):
    # Connect to the database
    global map_plot
    collection = connect_to_db()
    map_plot = local_map_plot
    if collection is None:  # Explicit None check
        return map_plot

    # Ensure the index exists
    ensure_2dsphere_index(collection)

    # Read the CSV file
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} points from {csv_path}")
    except Exception as e:
        print(f"Error reading CSV file {csv_path}: {e}")
        return map_plot

    # Query and plot the first 'num_points' coordinates
    plotted_buildings = 0
    for i, row in df.head(num_points).iterrows():
        try:
            lat, lon = row["latitude"], row["longitude"]
            addMarker(lat, lon, row["address"])
            # print(f"\nQuerying point {i+1}/{num_points}: ({lon}, {lat})")
            building = query_closest_point(collection, lon, lat)

            if building and "geometry" in building and building["geometry"]["type"] == "Polygon":
                # Extract coordinates from the geometry (first ring of the polygon)
                coords = building["geometry"]["coordinates"][0]
                tags = building.get("tags", {})
                centroid = building["centroid"]

                # Prepare popup text
                street = tags.get("addr:street", "Unnamed Building")
                housenumber = tags.get("addr:housenumber", "No house number")
                popup_text = f"Address: {housenumber} {street}"
                addMarker(centroid[1], centroid[0], street, color="black")

                # Plot the polygon on the map (folium expects [lat, lon])
                folium.Polygon(
                    locations=[(coord[1], coord[0])
                               # Swap lon, lat to lat, lon
                               for coord in coords],
                    popup=popup_text,
                    color="blue",
                    fill=True,
                    fill_opacity=0.4
                ).add_to(map_plot)
                plotted_buildings += 1
        except KeyError as e:
            print(f"Error: CSV missing required column {e}")
            return map_plot
        except Exception as e:
            print(f"Error processing row {i}: {e}")
            continue

    print(
        f"\nFinished querying {num_points} points. Plotted {plotted_buildings} buildings.")
    return map_plot

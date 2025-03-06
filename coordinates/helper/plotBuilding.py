# Save as plot_buildings.py
import pymongo
import folium


def plot_buildings(m: folium.Map, db_name="osm_ny", collection_name="buildings", limit=1000):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    collection = client[db_name][collection_name]

    # Get first 10,000 buildings
    buildings = list(collection.find().limit(limit))

    # Center map on NYC (approx)

    # Add buildings to map
    for building in buildings:
        coords = building["geometry"]["coordinates"][0]
        name = building["tags"].get("name", "Unnamed Building")
        folium.Polygon(
            locations=[(lat, lon)
                       for lon, lat in coords],  # Flip lon, lat for Folium
            popup=name,
            color="blue",
            fill=True,
            fill_opacity=0.4
        ).add_to(m)


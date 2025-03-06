# Save as plot_buildings.py
import pymongo
import folium

def plot_buildings(db_name="osm_ny", collection_name="buildings", output_file="buildings_map.html"):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    collection = client[db_name][collection_name]
    
    # Get all buildings (up to 1000)
    buildings = list(collection.find())
    
    # Center map on NYC (approx)
    m = folium.Map(location=[40.7128, -74.0060], zoom_start=12)
    
    # Add buildings to map
    for building in buildings:
        coords = building["geometry"]["coordinates"][0]
        name = building["tags"].get("name", "Unnamed Building")
        folium.Polygon(
            locations=[(lat, lon) for lon, lat in coords],  # Flip lon, lat for Folium
            popup=name,
            color="blue",
            fill=True,
            fill_opacity=0.4
        ).add_to(m)
    
    # Save map
    m.save(output_file)
    print(f"Map saved to {output_file}")

if __name__ == "__main__":
    plot_buildings()
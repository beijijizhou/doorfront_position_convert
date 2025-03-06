# Save as process_osm_to_mongo.py
import osmium
import pymongo
import time

def process_osm_to_mongo(input_file, db_name="osm_ny", collection_name="buildings", batch_size=1000):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    collection.drop()  # Clear existing data
    
    # Start timing
    start_time = time.time()
    processed_nodes = 0
    building_count = 0
    batch = []
    
    class BuildingLoader(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.node_coords = {}
        
        def node(self, n):
            nonlocal processed_nodes
            processed_nodes += 1
            self.node_coords[n.id] = (n.location.lon, n.location.lat)
            
            if processed_nodes % 100000 == 0:
                elapsed = time.time() - start_time
                print(f"Processed {processed_nodes} nodes in {elapsed:.2f}s "
                      f"({processed_nodes/elapsed:.0f} nodes/s), "
                      f"Buildings so far: {building_count}")
        
        def way(self, w):
            nonlocal building_count, batch
            if "building" in w.tags:
                coords = [self.node_coords[n.ref] for n in w.nodes if n.ref in self.node_coords]
                
                if coords and len(coords) > 1 and coords[0] == coords[-1]:  # Closed polygon
                    geometry = {
                        "type": "Polygon",
                        "coordinates": [[ [lon, lat] for lon, lat in coords ]]
                    }
                    doc = {
                        "osm_id": w.id,
                        "geometry": geometry,
                        "tags": dict(w.tags)
                    }
                    batch.append(doc)
                    building_count += 1
                    
                    if len(batch) >= batch_size:
                        collection.insert_many(batch)
                        batch = []

    print(f"Starting processing of {input_file} (processing all buildings)...")
    loader = BuildingLoader()
    loader.apply_file(input_file)
    
    # Insert remaining batch
    if batch:
        collection.insert_many(batch)
    
    # Create 2dsphere index
    collection.create_index([("geometry", "2dsphere")])
    
    process_time = time.time() - start_time
    print(f"\nProcessing complete in {process_time:.2f} seconds")
    print(f"Total buildings stored: {building_count}")
    print(f"Processed nodes: {processed_nodes}")
    print(f"Average processing speed: {processed_nodes/process_time:.0f} nodes/s")
    client.close()

if __name__ == "__main__":
    osm_file = "/Users/hongzhonghu/Downloads/NewYork.osm.pbf"
    process_osm_to_mongo(osm_file)
import geopandas as gpd
import folium
from shapely.geometry import Point, LineString
import uuid

def plot_csv_data_with_arrows( point_buffer=10, num_rows=200):
    name_extension = str(uuid.uuid4())
    output_file = f"{name_extension} corrected data map with arrows.html"
    old_file_path = "./corrected_doorfront_data.csv"
    new_file_path = "./doorfront.csv"
    
    # Read old and new data
    old_data = gpd.read_file(old_file_path).head(num_rows)
    new_data = gpd.read_file(new_file_path).head(num_rows)
    
    # Convert old and new data to GeoDataFrames
    old_data['geometry'] = old_data.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    new_data['geometry'] = new_data.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
    
    old_gdf = gpd.GeoDataFrame(old_data, geometry='geometry', crs="EPSG:4326").to_crs("EPSG:2263")
    new_gdf = gpd.GeoDataFrame(new_data, geometry='geometry', crs="EPSG:4326").to_crs("EPSG:2263")
    
    # Create buffered GeoDataFrames
    old_buffered_gdf = old_gdf.copy()
    old_buffered_gdf['geometry'] = old_buffered_gdf.geometry.buffer(point_buffer)
    
    new_buffered_gdf = new_gdf.copy()
    new_buffered_gdf['geometry'] = new_buffered_gdf.geometry.buffer(point_buffer)
    
    # Create arrows (LineString geometries) from old to new points
    arrow_lines = []
    for old_point, new_point in zip(old_gdf.geometry, new_gdf.geometry):
        arrow_lines.append(LineString([old_point, new_point]))
    arrow_gdf = gpd.GeoDataFrame(geometry=arrow_lines, crs=old_gdf.crs)
    
    # Plot the data
    m = old_gdf.explore(color='black', name="Old Data")
    new_gdf.explore(m=m, color='red', name="New Data")
    arrow_gdf.explore(m=m, color='blue', name="Arrows")
    folium.LayerControl().add_to(m)
    m.save(output_file)
    print("Finished map with arrows")
    return m

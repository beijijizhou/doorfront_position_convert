import uuid
import geopandas as gpd
from shapely.geometry import Point, LineString
import folium


def load_and_prepare_data(file_path, num_rows, crs="EPSG:4326", target_crs="EPSG:2263"):
    data = gpd.read_file(file_path).head(num_rows)
    data['geometry'] = data.apply(lambda row: Point(
        row['longitude'], row['latitude']), axis=1)
    gdf = gpd.GeoDataFrame(data, geometry='geometry', crs=crs)
    return gdf.to_crs(target_crs)


def create_arrows(old_gdf, new_gdf):
    arrows = []
    for old, new in zip(old_gdf.geometry, new_gdf.geometry):
        if not old.is_empty and not new.is_empty:
            arrows.append(LineString([old, new]))
    return gpd.GeoDataFrame(geometry=arrows, crs=old_gdf.crs)


def plot_csv_data_with_arrows(point_buffer=10, num_rows=200):

    output_file = f"corrected_data_map_with_arrows.html"

    old_file_path = "./corrected_doorfront_data.csv"
    new_file_path = "./doorfront.csv"

    old_gdf = load_and_prepare_data(old_file_path, num_rows)
    new_gdf = load_and_prepare_data(new_file_path, num_rows)

    arrow_gdf = create_arrows(old_gdf, new_gdf)

    # Initialize the map with the first point's location
    m = old_gdf.explore(color='black', name="Old Data")
    # new_gdf.explore(m=m, color='red', name="New Data")
    # arrow_gdf.explore(m=m, color='blue', name="Arrows")
    for idx, row in old_gdf.iterrows():
        folium.Marker(location=[row.geometry.y, row.geometry.x]).add_to(m)

    
    m.save(output_file)
    print(f"Finished map with arrows: {output_file}")
    return m

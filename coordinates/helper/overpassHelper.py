import overpy
import folium
from typing import List, Tuple

import pandas as pd

from geojson_reader import time_function


@time_function
def fetch_building_shapes(lat: float, lon: float, radius: int = 10) -> List[List[Tuple[float, float]]]:
    api = overpy.Overpass()
    query = f"""
    [out:json];
    (
      way["building"](around:{radius}, {lat}, {lon});
    );
    (._; >;);
    out body;
    """
    result = api.query(query)
    return [[(node.lat, node.lon) for node in way.nodes] for way in result.ways if way.nodes]

@time_function
def get_overpass_buildings(data: pd.DataFrame, map_plot: folium.Map) -> None:
    for index, row in data.iterrows():
        lat = row['latitude']
        lon = row['longitude']
        buildings = fetch_building_shapes(lat, lon)
        
        for building in buildings:
            folium.Polygon(locations=building, color="blue", fill=True, fill_opacity=0.4).add_to(map_plot)


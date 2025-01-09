from shapely.geometry import Point
import pyproj

# Define the transformer from CRS 4326 to the target CRS (e.g., 2263)
transformer = pyproj.Transformer.from_crs(4326, 2263, always_xy=True)

# Input latitude and longitude
lat = 40.7667734
lng = -73.9808934

# Transform the point
x, y = transformer.transform(lng, lat)

# Create a Shapely Point in the target CRS
geo_point = Point(x, y)

# Print the results
print(lat, lng, geo_point)

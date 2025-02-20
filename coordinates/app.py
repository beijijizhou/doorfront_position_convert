
from flask import Flask, render_template_string
import folium
from helper.geojsonHelper import plot_all_geojson_address
from helper.nominatimHelper import get_all_nominatim_address
from helper.fileHelper import read_data
from helper.overpassHelper import get_overpass_buildings
from helper.google_helper import plot_google_file, read_and_get_google_address
from plot import create_map_with_markers, plot_google_data
app = Flask(__name__)


@app.route('/')
def map_view():
    # Path to your CSV files
    old_data_path = 'doorfront.csv'
    full_data_path = 'full corrected_doorfront_data.csv'
    csv_file_path = "geojson.csv"
    output_file_path = "reverse_geocoded_results.csv"
    google_path = "google_address.csv"
    # Generate the map HTML
    # map_html = create_map_with_markers(old_data_path)._repr_html_()
    # df = compare_house_numbers("updated_coordinates.csv")
    # google_data = read_and_get_google_address(full_data_path)
    map_plot = folium.Map(location=[40.730610, -73.935242], zoom_start=12)
    data = read_data(full_data_path)
    # get_overpass_buildings(data, map_plot)
    # print(data)
    plot_all_geojson_address(map_plot)
    get_all_nominatim_address(data, map_plot)
    plot_google_file(map_plot)
    return map_plot._repr_html_()


if __name__ == '__main__':
    app.run(debug=True)

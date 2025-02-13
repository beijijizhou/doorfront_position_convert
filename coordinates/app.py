
from flask import Flask, render_template_string
from plot import create_map_with_markers
app = Flask(__name__)


@app.route('/')
def map_view():
    # Path to your CSV files
    old_data_path = 'doorfront.csv'
    full_data_path = 'full corrected_doorfront_data.csv'
    csv_file_path = "geojson.csv"
    output_file_path = "reverse_geocoded_results.csv"
    # Generate the map HTML
    map_html = create_map_with_markers(old_data_path)._repr_html_()
    # df = compare_house_numbers("updated_coordinates.csv")
    # map_html = get_google_address(full_data_path)._repr_html_()
   
    return render_template_string(map_html)


if __name__ == '__main__':
    app.run(debug=True)

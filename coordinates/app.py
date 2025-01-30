
from flask import Flask, render_template_string
from file_utility import compare_house_numbers
from plot import create_map_with_markers
from google_helper import get_google_address
app = Flask(__name__)


@app.route('/')
def map_view():
    # Path to your CSV files
    new_data_path = 'corrected_doorfront_data.csv'
    old_data_path = 'doorfront.csv'
    full_data_path = 'full corrected_doorfront_data.csv'
    # Generate the map HTML
    map_html = create_map_with_markers(new_data_path, old_data_path)._repr_html_()
    df = compare_house_numbers("updated_coordinates.csv")
    # map_html = get_google_address(full_data_path)._repr_html_()
    # Render the map in the browser
    return render_template_string(map_html)


if __name__ == '__main__':
    print("reload")
    app.run(debug=True)


from flask import Flask, render_template_string
from plot import create_map_with_markers

app = Flask(__name__)

@app.route('/')
def map_view():
    # Path to your CSV files
    new_data_path = 'corrected_doorfront_data.csv'
    old_data_path = 'doorfront.csv'
    
    # Generate the map HTML
    map_html = create_map_with_markers(new_data_path, old_data_path)._repr_html_()
    
    # Render the map in the browser
    return render_template_string(map_html)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)


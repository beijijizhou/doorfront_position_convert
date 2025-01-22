# doorfront_position_convert


## MUST KNOW

### Overview
This project involves working with geospatial data in two different Coordinate Reference Systems (CRS):

1. **CRS 4326 (WGS 84)**:
   - Format: Latitude and Longitude in decimal degrees.
   - Example: `40.7667734, -73.9808934`
   - Commonly used for global mapping and GPS data.

2. **CRS 2263 (NAD83 / New York Long Island)**:
   - Format: Projected coordinates in feet (X, Y).
   - Example: `POINT (989562.379 218653.353)`
   - Optimized for high-accuracy mapping in New York City and surrounding areas.

### Transformations
- **From CRS 4326 to CRS 2263**: Converts latitude/longitude to X/Y coordinates for spatial calculations.
- **From CRS 2263 to CRS 4326**: Converts X/Y coordinates back to latitude/longitude for readability or integration with other systems.

### Important Notes
- Ensure the correct CRS is applied when creating or processing geospatial data.
- Use the provided transformation functions to switch between CRS 4326 and CRS 2263 as needed.
- Example Transformation:
  - Input (CRS 4326): `40.7667734, -73.9808934`
  - Output (CRS 2263): `POINT (989562.379 218653.353)`


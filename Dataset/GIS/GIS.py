r"""°°°
# GIS Data
°°°"""
# |%%--%%| <xWcH6eeVWy|mvVQZMaSva>

import osmnx as ox
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# |%%--%%| <mvVQZMaSva|x1pFPIO9fz>

# osmnx caching
ox.settings.use_cache = True
ox.settings.log_console = True

# |%%--%%| <x1pFPIO9fz|OZphKeI9aT>

# Define the place
place_name = 'Da Nang, Vietnam'

# |%%--%%| <OZphKeI9aT|JXAoxvcAzt>

# Get the boundary
try:
    place_gdf = ox.geocode_to_gdf(place_name)
    print("Successfully geocoded Da Nang.")
except Exception as e:
    print("Error geocoding {place_name}: {e}")

# |%%--%%| <JXAoxvcAzt|bcyvbbD1BI>

# plot the boundary
fig, ax = plt.subplots(figsize=(10, 10))
place_gdf.plot(ax=ax, fc="gray", ec="none")
ax.set_title(f"Boundary of {place_name}")
plt.show()

# Hold the actual shape of Da Nang boundary
place_polygon = place_gdf["geometry"].iloc[0]

# |%%--%%| <bcyvbbD1BI|tSi8A3eriQ>
r"""°°°
# Fetching Road Network
°°°"""
# |%%--%%| <tSi8A3eriQ|1FaFlaOO44>
r"""°°°
Downloads the road network for Da Nang and converts it into a graph structure (nodes and edges).

Here, intersections are nodes, and road segments are edges.
°°°"""
# |%%--%%| <1FaFlaOO44|MYAv57eUct>

# Get the driveable road network
road_network = ox.graph_from_place(place_name, network_type="drive_service", retain_all=True, truncate_by_edge=True)
# For a wider area or more detail if place_name is too restrictive:
# G = ox.graph_from_polygon(place_polygon, network_type="drive_service", retain_all=True, truncate_by_edge=True)

# Convert graph to GeoDataFrames
nodes, edges = ox.graph_to_gdfs(road_network)
print(f"Fetched {len(nodes)} nodes and {len(edges)} deges.")

# |%%--%%| <MYAv57eUct|wfsL0zBxWF>

print("\nRoad Network Edges Info:")
print(edges.info())
print("\nRoad Network Edges Head:")
edges.head(5)

# |%%--%%| <wfsL0zBxWF|choCwx4uIN>

# plot the road network
fig, ax = ox.plot_graph(road_network)

# |%%--%%| <choCwx4uIN|pemsYRj0jM>
r"""°°°
# Buildings and POIs
Downloads geographic features representing buildings and various Points of Interest (POIs) within Da Nang's boundary.
- Buildings: Provide context about land use (residential, commercial, industrial). Children might be in or near specific building types.
- POIs: Indicate places children might go to or be near (schools, parks, shops, transport hubs).
°°°"""
# |%%--%%| <pemsYRj0jM|kEnsAdSBGV>

feature_tags = {"building": True}
tags_pois = {
    "amenity": True,  # E.g., school, hospital, restaurant, parking
    "shop": True,     # E.g., supermarket, clothes, electronics
    "leisure": True,  # E.g., park, playground, sports_centre
    "tourism": True,  # E.g., hotel, museum, viewpoint
    "public_transport": True # E.g., bus_stop, station
}

buildings = ox.features_from_polygon(place_polygon, tags=feature_tags)
print(f"\nFetched {len(buildings)} building features")
# Fetching POIs
pois = ox.features_from_polygon(place_polygon, tags=tags_pois)
print(f"\nFetched {len(pois)} POI features.")

# |%%--%%| <kEnsAdSBGV|UGmFvFzNmP>

# Check if the building exist
if not buildings.empty:
    print(buildings.info())
    # We only want actual building areas, so filter for Polygon or MultiPolygon geometries
    buildings = buildings[buildings.geometry.type.isin(['Polygon', 'MultiPolygon'])]
buildings.iloc[15:20]

# |%%--%%| <UGmFvFzNmP|CXzwoN0plT>

if not pois.empty:
    print(pois.info())
pois.head()

# |%%--%%| <CXzwoN0plT|f9xtFuo9gB>
r"""°°°
# Initial GIS Data Cleaning and Structuring
°°°"""
# |%%--%%| <f9xtFuo9gB|BkWIEiPp5c>
r"""°°°
- Changes all geographic data to a consistent projected CRS (Coordinate Reference System).
- Fills in some missing values (for example: building type) + Create a single "pot_type" column.
- Store the processed GeoDataFrame inside Parquet files for reuse.
°°°"""
# |%%--%%| <BkWIEiPp5c|idcJnUOnWt>

TARGET_CRS = "EPSG:32648" # UTM Zone 48N.

if not buildings.empty:
    # Converts the GeoDataFrame's coordinate system.
    # For accurate distance/area calculations, it's better to use a projected CRS like UTM.
    buildings = buildings.to_crs(TARGET_CRS)
    buildings['building'] = buildings['building'].fillna('unknown') # Handle missing building types
    # Save to Parquet format
    buildings.to_parquet("danang_buildings_processed.parquet")

if not pois.empty:
    pois = pois.to_crs(TARGET_CRS)
    # Consolidate POI types for simplicity.
    # This creates a new 'poi_type_raw' by taking the first non-null value from the listed amenity/shop etc. columns for each row.
    poi_cols = ['amenity', 'shop', 'leisure', 'tourism', 'public_transport']
    pois['poi_type_raw'] = pois[poi_cols].bfill(axis=1).iloc[:, 0]
    pois['poi_type'] = pois['poi_type_raw'].fillna('unknown') # Fill any remaining NaNs
    # Save to Parquet format
    pois.to_parquet("danang_pois_processed.parquet")

edges = edges.to_crs(TARGET_CRS)

# Save intermediate files
# Saves a DataFrame/GeoDataFrame to Parquet format, which is efficient for storage and speed.
# edges.to_parquet("danang_edges_processed.parquet")
place_gdf.to_crs(TARGET_CRS).to_parquet("danang_boundary_processed.parquet")

# |%%--%%| <idcJnUOnWt|WGDLIBHsC1>
r"""°°°
# Gridding Da Nang
Divides the geographic area of Da Nang into a grid of uniform square cells (e.g., 100m x 100m). Each cell gets a unique ID. The AI model (LSTM) will predict the probability of the child being in one of these specific grid cells. This discretizes continuous space into manageable units.
°°°"""
# |%%--%%| <WGDLIBHsC1|FmEoBsjpUv>

from shapely.geometry import Polygon # For creating polygon shapes

# Define grid cell size in meters
CELL_SIZE_METERS = 100

# Get bounding box of Da Nang in the target CRS (UTM, so units are meters)
place_polygon_for_gridding = place_gdf["geometry"].iloc[0] # Place polygon in meters (re-extract)
minx, miny, maxx, maxy = place_polygon_for_gridding.bounds

# Create lists of x and y coordinates for the grid lines
x_coords = list(range(int(minx), int(maxx) + CELL_SIZE_METERS, CELL_SIZE_METERS))
y_coords = list(range(int(miny), int(maxy) + CELL_SIZE_METERS, CELL_SIZE_METERS))
print(len(x_coords), len(y_coords))

grid_cells_list = []
grid_id_counter = 0
# Loop through the x and y coordinates to create cell polygons
for i in range(len(x_coords) - 1):
    for j in range(len(y_coords) - 1):
        # Define the four corners of a cell
        cell_poly = Polygon([
            (x_coords[i], y_coords[j]),
            (x_coords[i+1], y_coords[j]),
            (x_coords[i+1], y_coords[j+1]),
            (x_coords[i], y_coords[j+1])
        ])
        # Only include cells whose centroid is within Da Nang or that intersect Da Nang
        # This avoids creating grid cells far outside the actual city area.
        if cell_poly.centroid.within(place_polygon_for_gridding) or cell_poly.intersects(place_polygon_for_gridding):
             grid_cells_list.append({"grid_id": f"cell_{grid_id_counter}", "geometry": cell_poly})
             grid_id_counter += 1

# Create a GeoDataFrame from the list of cell dictionaries
grid_gdf = gpd.GeoDataFrame(grid_cells_list, crs=TARGET_CRS)
print(f"\nCreated {len(grid_gdf)} grid cells...")

grid_gdf.to_parquet("danang_grid.parquet")

# |%%--%%| <FmEoBsjpUv|FUWisAI3iY>

# Load the Da Nang boundary
place_gdf = gpd.read_parquet("danang_boundary_processed.parquet")
# Load the created grid
grid_gdf = gpd.read_parquet("danang_grid.parquet")

# Plotting
fig, ax = plt.subplots(1, 1, figsize=(12, 12))
place_gdf.plot(ax=ax, facecolor='lightgray', edgecolor='black', linewidth=1, label='Da Nang Boundary', alpha=0.8)
# Plot the grid cells on top
grid_gdf.plot(ax=ax, facecolor="none", edgecolor='blue', linewidth=0.5, label='Grid Cells')

# --- Customize the plot (optional) ---
ax.set_xlabel("Easting (meters - UTM Zone)", fontsize=12) # Assuming UTM projection
ax.set_ylabel("Northing (meters - UTM Zone)", fontsize=12) # Assuming UTM projection

plt.tight_layout()

# Display the plot
plt.show()

print(f"Plotted {len(grid_gdf)} grid cells over the Da Nang boundary.")

# |%%--%%| <FUWisAI3iY|5cHoG8hkoX>
r"""°°°
# Feature Engineering: Associating GIS Data with Grid Cells.
°°°"""
# |%%--%%| <5cHoG8hkoX|k7JkgHNNZ7>
r"""°°°
For each cell, it calculates features like:
- The dominant building type (e.g., residential, commercial).
- Counts of different building types.
- Counts of different POI types (e.g., number of schools, parks).
- Total road length within the cell and road density.
- Converts categorical features (like dominant building type) into a numerical format (one-hot encoding) suitable for machine learning.
°°°"""
# |%%--%%| <k7JkgHNNZ7|IHbFldwweL>



import math
from operator import itemgetter
from pathlib import Path
import folium
from tableauscraper import TableauScraper as TS
import pandas as pd
import numpy as np
from gtfslib import GTFS, CoordsUtil, Projections
import re
import colorsys

url = "https://tableau.dart.org/t/Public/views/DARTscorecard/DARTScorecard"
sheetName = "Ridership label KPI"
subsheetName = "by Route for DART Bus Service"

data_folder = Path("data")
export_folder = Path("export")
output_file = Path("map.html")

min_color = np.array((0, 1.0, 1.0))
max_color = np.array((0.333, 1.0, 0.8))

def scale_color(scaled_num):
    color_hsv = (max_color - min_color) * scaled_num + min_color
    color_rgb = [round(i * 255) for i in colorsys.hsv_to_rgb(*color_hsv)]
    color_hex = f"#{color_rgb[0]:02x}{color_rgb[1]:02x}{color_rgb[2]:02x}"
    return color_hex

data_folder.mkdir(parents=True, exist_ok=True)
export_folder.mkdir(parents=True, exist_ok=True)

print("Loading tableau...")
ts = TS()
ts.loads(url)

workbook = ts.getWorkbook()
ridershipSheet = ts.getWorksheet(sheetName)
dashboard = ridershipSheet.select("Ridership label", "Ridership Performance")

serviceByRoute = dashboard.getWorksheet(subsheetName)
data: pd.DataFrame = serviceByRoute.data
data.to_csv(export_folder / "tableau.csv")

columns = [
    "Route-value",
    "MEASURE_CODE-value",
    "SERVICE_CATEGORY-value",
    "SUM(MEASURE_VALUE)-value",
]

data["SUM(MEASURE_VALUE)-value"] //= 30 # monthly to daily average

filtered = data[data["SUM(MEASURE_VALUE)-value"]>0].filter(columns, axis="columns")
def get_route_name(name):
    m = re.search(r"\((.*)\)", name)
    if m:
        return m.group(1)
    if name == "TI":
        return "TI SHUTTLE"
    if name == "UTS":
        return "UT SOUTHWESTERN"

    print("unable to find route with name", name)
    return name

measure_values = filtered["SUM(MEASURE_VALUE)-value"]
measure_min = np.min(measure_values)
measure_max = np.max(measure_values)

print(measure_min, measure_max)

print("Loading GTFS...")
gtfs = GTFS(data_folder / "dart_gtfs.zip")
routes = gtfs.routes
routes.to_csv(export_folder / "gtfs_routes.csv")

map_routes = []
for index, row in filtered.iterrows():
    route_name = get_route_name(row['Route-value'])
    matching_routes = routes[routes["route_long_name"] == route_name]
    if len(matching_routes) == 0:
        matching_routes = routes[routes["route_long_name"].str.contains(route_name)]
    matching_route_ids = matching_routes["route_id"]
    riders = int(row["SUM(MEASURE_VALUE)-value"])
    scaled_riders = (riders - measure_min) / (measure_max - measure_min) # between 0-1
    scaled_riders = math.sqrt(scaled_riders) # boost lower values
    color_hex = scale_color(scaled_riders)
    for rid in matching_route_ids:
        map_routes.append((rid, riders, { "daily_ridership": riders, "color": color_hex, "route_desc": None, "route_type": None, "route_text_color": None, "route_url": None }))

map_routes.sort(key=itemgetter(1))
route_info = {route[0]: route[2] for route in map_routes}

print(f"Generating map into {output_file.name}")
route_map = gtfs.get_map(route_info)

import restapi
SERVER_URL = (
    "https://geospatial.nctcog.org/server/rest/services"
    # + "/Transportation/TrafficCounts/MapServer"
)
ags = restapi.ArcServer(SERVER_URL)
traffic_counts_service = ags.getService("TrafficCounts")
traffic_counts = traffic_counts_service.layer(0)
query = traffic_counts.query(where="Date>=Date'2021-01-01'", exceed_limit=True)

import geopandas as gpd
traffic_gdf = gpd.GeoDataFrame.from_features(query.json["features"])
traffic_gdf.set_crs(Projections.WGS84, inplace=True)


filtered_route_gdf = gtfs._routes_by_id[gtfs._routes_by_id.index.isin(list(map(itemgetter(0), map_routes)))]


traffic_counts_x_routes = gpd.sjoin(
    traffic_gdf,
    CoordsUtil.buffer_points(10, filtered_route_gdf).to_crs(Projections.WGS84),
)


min_count = math.sqrt(traffic_counts_x_routes["Count24hr"].min())
max_count = math.sqrt(traffic_counts_x_routes["Count24hr"].max())

heat_data = []

traffic_count_group = folium.FeatureGroup(name="Traffic Counts")
for idx, row in traffic_counts_x_routes.iterrows():
    count = row["Count24hr"]
    road = (row["Roadway"] or "Unnamed Road")
    label = row["LABEL"] or ""
    if pd.isna(count):
        continue
    paired_route_id = row["route_id"]
    short_name = gtfs._routes_by_id.loc[paired_route_id]["route_short_name"]
    daily_riders = route_info[paired_route_id]["daily_ridership"]
    bus_efficiency = daily_riders / count if count > 0 else 0
    if bus_efficiency > 1 or count < 1000:
        continue
    
    eff_color = scale_color(math.sqrt(bus_efficiency))
    
    heat_data.append([row.geometry.y, row.geometry.x, bus_efficiency**2 * 2])
    
    logcount = math.sqrt(count)
    folium.CircleMarker(
        location=[row.geometry.y, row.geometry.x],
        radius=math.ceil((logcount - min_count) / (max_count - min_count) * 7) + 3,
        color=eff_color,
        fill=True,
        fill_color=eff_color,
        fill_opacity=0.7,
        popup=folium.Popup(f"{road}<br>{label}<br>Route {short_name} Efficiency: {bus_efficiency*100:.01f}%", max_width=300),
    ).add_to(traffic_count_group)

traffic_count_group.add_to(route_map)

from folium import plugins
plugins.HeatMap(
    heat_data, name="Bus Efficiency data point", radius=15, blur=0, show=False
).add_to(route_map, index=0)


folium.TileLayer("openstreetmap", name="OpenStreetMap", show=False).add_to(route_map)
folium.LayerControl(collapsed=False).add_to(route_map)

# Define the HTML and CSS for the footer
footer_html = """
<div style="position: fixed;
            bottom: 0px;
            left: 0px;
            width: 100%;
            background-color: white;
            color: black;
            text-align: center;
            padding: 5px;
            z-index: 1000;
            font-size: 14px;">
    Made with <3 by <a href="https://github.com/democat3457">Colin Wong</a>. Data retrieved from <a href="https://www.dart.org/about/about-dart/key-performance-indicator">DART Scorecard</a>, <a href="https://www.dart.org/about/about-dart/fixed-route-schedule">DART GTFS data</a>, and <a href="https://trafficcounts.nctcog.org/">NCTCOG Traffic Counts</a>. Map generated using folium.
</div>
"""

# Add the footer HTML element to the map's HTML root
route_map.get_root().html.add_child(folium.Element(footer_html))

route_map.save(str(output_file.resolve()))

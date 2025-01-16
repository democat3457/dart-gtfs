# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: 3.10.13
#     language: python
#     name: python3
# ---

# %% [markdown]
# DART GTFS data: https://www.dart.org/about/about-dart/fixed-route-schedule  
# https://www.dart.org/transitdata/latest/google_transit.zip

# %%
from pathlib import Path
import gtfs_kit as gk
import folium
from datetime import time, timedelta
import pandas as pd
import geopandas as gpd

data_folder = Path("../data")
if not data_folder.exists():
    data_folder.mkdir()
export_folder = Path("../export")
if not export_folder.exists():
    export_folder.mkdir()
file = Path(data_folder) / "google_transit.zip"

# %%
import requests
import shutil

r = requests.get("https://www.dart.org/transitdata/latest/google_transit.zip", stream=True)
if r.status_code == 200:
    with file.open("wb") as out_file:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, out_file)

# %%
Path().cwd()

# %%
feed = gk.read_feed(file, dist_units="mi")
descrip = feed.describe()
descrip[descrip["indicator"]=="end_date"]["value"].iat[0]

# %%
routes = feed.routes
for x in routes[routes["route_long_name"].str.contains("ROSS")]["route_id"]:
    print(type(x), x)

# %%
feed.stops

# %%
pnt1 = Point(80.99456, 7.86795)
pnt2 = Point(80.97454, 7.872174)
points_df = gpd.GeoDataFrame({"geometry": [pnt1, pnt2]}, crs="EPSG:4326")
points_df = points_df.to_crs("EPSG:5234")
points_df2 = points_df.shift()  # We shift the dataframe by 1 to align pnt1 with pnt2
points_df.distance(points_df2).iloc[1]

# %%
from pykml import parser
with (data_folder / "Boundary.kml").open() as f:
    doc = parser.parse(f)

boundary_coords = [ tuple(map(float, line.strip().split(',')))[1::-1] for line in doc.findall(".//{http://www.opengis.net/kml/2.2}Placemark")[0].find(
    ".//{http://www.opengis.net/kml/2.2}coordinates"
).text.strip().splitlines() ]

# %%
stop = "22750"
from shapely.geometry import Point
row = feed.stops[feed.stops["stop_id"] == stop].iloc[0]
lat, lon = row["stop_lat"], row["stop_lon"]
gdf = gpd.GeoDataFrame({'geometry': [Point(lon, lat), Point(lon+2, lat-2)]}, crs='EPSG:4326')
fgds = gdf.to_crs('EPSG:6583').buffer(1900*3).to_crs('EPSG:4326')
fgdf = gpd.GeoDataFrame(geometry=fgds)
stops = feed.get_stops_in_area(fgdf)

print(stops)

m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

for idx, stop in stops.iterrows():
    name, lat, lon = stop["stop_name"], stop["stop_lat"], stop["stop_lon"]
    folium.Circle(
        location=[lat, lon],
        popup=name,
        # fill_color="#f00",
        # fill_opacity=0.5,
        # weight=0,
        radius=50,
    ).add_to(m)

# folium.Polygon(locations=boundary_coords, color="black", fill=False).add_to(m)
m

# %%
m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

for idx, stop in feed.stops.iterrows():
    name, lat, lon = stop["stop_desc"], stop["stop_lat"], stop["stop_lon"]
    folium.Circle(location=[lat, lon], popup=name, fill_color="#f00", fill_opacity=1, weight=0, radius=804.672).add_to(m)

folium.Polygon(locations=boundary_coords, color="black", fill=False).add_to(m)

m

# %%
# Choose study dates

week = feed.get_first_week()
dates = [week[4], week[6]]  # First Friday and Sunday
dates

# Build a route timetable

route_id = feed.routes["route_id"].iat[0]
feed.build_route_timetable(route_id, dates).T

# %%
rids = feed.routes.route_id.loc[:]
print(feed.routes.route_id.loc[0])
feed.routes.to_csv(export_folder / "routes.csv")
feed.stops.to_csv(export_folder / "stops.csv")
# feed.map_routes(rids, show_stops=False)

# %%
trip_stats = feed.compute_trip_stats()
feed.compute_route_stats(trip_stats, ["20241216"]).to_csv(export_folder / "route_stats_20241216.csv")

# %%
feed.compute_route_time_series(trip_stats, ["20241216"]).to_csv(export_folder / "route_time_series_20241216.csv")

# %%
akard_timetable = feed.build_stop_timetable("22750", ["20241216"])
akard_timetable

# %%
tt = akard_timetable.copy()
tt["arrival_time"] = pd.to_timedelta(tt["arrival_time"])
tt["departure_time"] = pd.to_timedelta(tt["departure_time"])

# %%
tt["arrival_time"].iat[-1] < timedelta(hours=20)

# %%
nn = tt[pd.notna(tt["arrival_time"])]
nn[(nn["departure_time"] >= timedelta(hours=9)) & (nn["arrival_time"] <= timedelta(hours=11, minutes=30))]

# %%
mask = pd.notna(nn["departure_time"]) & pd.notna(nn["arrival_time"])
mask &= nn["departure_time"] >= timedelta(hours=9)
mask &= nn["arrival_time"] <= timedelta(hours=11, minutes=30)
nn[mask]

# %%
feed.build_route_timetable('25826', ['20241216'])

# %%
dtstf = feed.append_dist_to_stop_times()

# %%
print(dtstf)

# %%
feed.stop_times

# %%
feed.stop_times[feed.stop_times["trip_id"] == "8211247"]

# %%
feed.stop_times[feed.stop_times["timepoint"]==1]

# %%
feed.trips[feed.trips["trip_id"]=='8211247']

# %%
feed.trips[feed.trips["route_id"] == "25753"]

# %%
feed.shapes

# %%
feed.calendar_dates

# %%
feed.build_route_timetable("25753", ["20241219"])

# %%
feed.feed_info.to_json()

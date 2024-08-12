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
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# DART GTFS data: https://www.dart.org/about/about-dart/fixed-route-schedule
# https://www.dart.org/transitdata/latest/google_transit.zip

# %%
from pathlib import Path
import gtfs_kit as gk

# %%
file = Path("google_transit.zip")

feed = gk.read_feed(file, dist_units="mi")
feed.describe()

# %%
routes = feed.routes
for x in routes[routes["route_long_name"].str.contains("ROSS")]["route_id"]:
    print(type(x))

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
feed.routes.to_csv("routes.csv")
# feed.map_routes(rids, show_stops=False)

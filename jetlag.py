from __future__ import annotations

import functools
import heapq

import pandas as pd
import shapely
from tqdm import tqdm
from gtfslib import GTFS, CoordsUtil, Projections, RouteType
from pathlib import Path
import folium
from datetime import datetime, timedelta, date, time
import time as pytime

import requests
import shutil

START_TIME = datetime(2024, 12, 16, 9, 0, 0)
HIDE_DURATION = timedelta(minutes=90)
START_STOP = 22750 # Akard
WALKING_SPEED = 1.06 # m/s
ALLOWED_TRAVEL_MODES = RouteType.all()
ALLOWED_HIDING_MODES = [ RouteType.LIGHT_RAIL ]

def _dateformat(d):
    return d.strftime('%Y%m%d')

today = _dateformat(START_TIME)
end_time = START_TIME + HIDE_DURATION

data_folder = Path("data")
if not data_folder.exists():
    data_folder.mkdir()
export_folder = Path("export")
if not export_folder.exists():
    export_folder.mkdir()
file = Path(data_folder) / "google_transit.zip"

print("Initializing GTFS...")
_init_time = pytime.time()

download_file = True
if file.exists():
    gtfs = GTFS(file)
    _startdate = datetime.strptime(gtfs.get_description_value("start_date"), '%Y%m%d').date()
    _enddate = datetime.strptime(gtfs.get_description_value("end_date"), '%Y%m%d').date()
    if _startdate <= date.today() <= _enddate:
        download_file = False

if download_file:
    print("Downloading DART GTFS zip file...")
    r = requests.get(
        "https://www.dart.org/transitdata/latest/google_transit.zip", stream=True
    )
    if r.status_code == 200:
        with file.open("wb") as out_file:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, out_file)
    gtfs = GTFS(file)

# Access property to cache elements
gtfs.stop_route_types

_init_time_stop = pytime.time()
print(f"Finished initialization in {_init_time_stop-_init_time:.2f}s.")

StopId = str | int
TripId = str | int
StopSeq = int
Timeish = time | timedelta

def timedelta_coerce(t: Timeish):
    if isinstance(t, (timedelta, pd.Timedelta)):
        return t
    return datetime.combine(date.min, t) - datetime.combine(date.min, time())

def dt_minus_date(dt: datetime, d: date):
    return dt - datetime.combine(d, time())

@functools.lru_cache()
def get_stop_timetable(stop: StopId):
    tt = gtfs.build_stop_timetable(str(stop), [today])
    tt["arrival_time"] = pd.to_timedelta(tt["arrival_time"])
    tt["departure_time"] = pd.to_timedelta(tt["departure_time"])
    return tt


def df_time_bound(df: pd.DataFrame, lower: Timeish | None = None, upper: Timeish | None = None):
    mask = pd.notna(df["departure_time"]) & pd.notna(df["arrival_time"])
    if lower:
        mask &= df["departure_time"] >= timedelta_coerce(lower)
    if upper:
        mask &= df["arrival_time"] <= timedelta_coerce(upper)
    return df[mask]

def trips_between_for_stop(stop: StopId, t1: Timeish, t2: Timeish):
    tt = get_stop_timetable(stop)
    return df_time_bound(tt, t1, t2)


stop_times: pd.DataFrame = gtfs.feed.stop_times
stop_times["arrival_time"] = pd.to_timedelta(stop_times["arrival_time"])
stop_times["departure_time"] = pd.to_timedelta(stop_times["departure_time"])
stop_times_by_trip = stop_times.groupby(["trip_id"], sort=False)

def get_future_stops_on_trip(trip: TripId, stop_seq: StopSeq = 0):
    st = stop_times_by_trip.get_group((str(trip),))
    return st[st["stop_sequence"] > int(stop_seq)]


class RouteCombo:
    def __init__(self, *trips: str):
        self.trips: tuple[str, ...] = trips

    def append(self, trip: str) -> RouteCombo:
        return RouteCombo(*self.trips, trip)

    def get_popup_lines(self) -> list[str]:
        route_text = ["Steps:"]
        if not len(self.trips):
            route_text.append("We started here!")
        else:
            route_text += [" - " + route for route in self.trips]
        return route_text

    def get_last_trip(self) -> str | None:
        if not len(self.trips):
            return None
        return self.trips[-1]

    def __str__(self):
        return str(self.trips)

    def __iter__(self):
        return iter(self.trips)

    def __len__(self):
        return len(self.trips)

    def __lt__(self, other):
        if not isinstance(other, RouteCombo):
            raise TypeError
        return len(self.trips) < len(other.trips)

    def __gt__(self, other):
        if not isinstance(other, RouteCombo):
            raise TypeError
        return len(self.trips) > len(other.trips)

    def __hash__(self):
        return hash(self.trips)

    def __eq__(self, other):
        return isinstance(other, Path) and self.trips == other.trips


visited_stops: dict[str, tuple[datetime, RouteCombo]] = dict() # stop_id : first time we reach stop, fastest route combo
visited_trips: set[str] = set()

added_stops: dict[str, timedelta] = dict() # temp dict to stop adding to queue

end_timedelta = dt_minus_date(end_time, START_TIME.date())


queue = [(timedelta_coerce(START_TIME.time()), str(START_STOP), RouteCombo())] # time we get there, stop id, route combo
heapq.heapify(queue)

def push_to_queue(arrival_time: Timeish, stop_id: StopId, routes: RouteCombo):
    if stop_id in added_stops:
        if arrival_time > added_stops[stop_id]:
            # if stop has already been added and the tentative time is later than the already queued time, skip
            return
    heapq.heappush(queue, (arrival_time, stop_id, routes))
    added_stops[stop_id] = arrival_time

t = tqdm()
while len(queue):
    t.set_description(str(len(queue)), refresh=False)
    t.update()
    td, stop_id, routes = heapq.heappop(queue)
    if stop_id in visited_stops:
        continue
    if td > end_timedelta:
        continue
    visited_stops[stop_id] = datetime.combine(START_TIME.date(), time()) + td, routes
    stop_timetable = trips_between_for_stop(stop_id, td, end_timedelta)
    first_available_routes = stop_timetable.drop_duplicates('route_id', keep='first')
    # print(stop_timetable)
    for _, row_gdf in first_available_routes.iterrows():
        trip_id, stop_seq, trip_name = row_gdf["trip_id"], row_gdf["stop_sequence"], row_gdf["trip_headsign"]

        # only travel in allowed route types
        if gtfs.trip_route_types[trip_id] not in ALLOWED_TRAVEL_MODES:
            continue

        if trip_id in visited_trips:
            continue
        visited_trips.add(trip_id)
        # print(stop_id, row)
        for _, future_stop in get_future_stops_on_trip(trip_id, stop_seq).iterrows():
            arrival_time = timedelta_coerce(future_stop["arrival_time"])
            if arrival_time > end_timedelta:
                continue
            future_stop_id = future_stop["stop_id"]
            push_to_queue(arrival_time, future_stop_id, routes.append(trip_name))

    # if we had just walked, walking again is not going to provide new stations
    if routes.get_last_trip() == "walking":
        continue

    # walking calculation
    if WALKING_SPEED <= 0:
        continue
    remaining_time = end_timedelta - td
    walking_distance = WALKING_SPEED * remaining_time.seconds
    stop_df = gtfs.get_stop(stop_id)
    buffered_area = CoordsUtil.buffer_points(walking_distance, stop_df)
    stop_geometry = stop_df.iloc[0]["geometry"]
    stops_in_area = gtfs.get_stops_in_area(buffered_area)
    for _, row in stops_in_area.iterrows():
        distance_to_stop = shapely.distance(stop_geometry, row["geometry"])
        arrival_time = td + (distance_to_stop / WALKING_SPEED * timedelta(seconds=1))
        future_stop_id = row["stop_id"]
        push_to_queue(arrival_time, future_stop_id, routes.append("walking"))

t.close()

print(f'Evaluated {len(visited_trips)} trips and found {len(visited_stops)} reachable stops.')

# import pprint
# pprint.pprint(visited_stops)

m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

for stop_id, (dt, routes) in visited_stops.items():
    stop = gtfs.get_stop(stop_id).to_crs(Projections.WGS84).iloc[0]
    name, point = stop["stop_name"], stop.geometry
    lon, lat = point.x, point.y
    is_valid_hiding_spot = any(
        rtype in ALLOWED_HIDING_MODES
        for rtype in gtfs.stop_route_types[stop_id]
    )

    popup_lines = [
        name,
        f'Earliest time: {dt.strftime("%m/%d %H:%M:%S")}',
        '',
    ]
    popup_lines += routes.get_popup_lines()
    popup = folium.Popup(
        "<br>".join(popup_lines),
        max_width=300
    )

    folium.Circle(
        location=[lat, lon],
        tooltip=name,
        popup=popup,
        fill_color="#00f" if is_valid_hiding_spot else "#f00",
        fill_opacity=0.2,
        color="black",
        weight=1,
        radius=804.672 if is_valid_hiding_spot else 20,
    ).add_to(m)

m.save("jetlag.html")

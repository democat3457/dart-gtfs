from collections import defaultdict
import functools
import heapq

import pandas as pd
from tqdm import tqdm
from gtfslib import GTFS
from pathlib import Path
import folium
from datetime import datetime, timedelta, date, time

import requests
import shutil

START_TIME = datetime(2024, 12, 16, 9, 0, 0)
HIDE_DURATION = timedelta(minutes=90)
START_STOP = 22750 # Akard

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

download_file = True
if file.exists():
    gtfs = GTFS(file)
    _startdate = datetime.strptime(gtfs.get_description_value("start_date"), '%Y%m%d').date()
    _enddate = datetime.strptime(gtfs.get_description_value("end_date"), '%Y%m%d').date()
    if _startdate <= date.today() <= _enddate:
        download_file = False

if download_file:
    r = requests.get(
        "https://www.dart.org/transitdata/latest/google_transit.zip", stream=True
    )
    if r.status_code == 200:
        with file.open("wb") as out_file:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, out_file)
    gtfs = GTFS(file)

StopId = str | int
# RouteId = str | int
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
    tt = gtfs.feed.build_stop_timetable(str(stop), [today])
    tt["arrival_time"] = pd.to_timedelta(tt["arrival_time"])
    tt["departure_time"] = pd.to_timedelta(tt["departure_time"])
    return tt

# @functools.lru_cache()
# def get_route_timetable(route: RouteId):
#     tt = gtfs.feed.build_route_timetable(str(route), [today])
#     tt["arrival_time"] = pd.to_timedelta(tt["arrival_time"])
#     tt["departure_time"] = pd.to_timedelta(tt["departure_time"])
#     return tt

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

# def trips_between_for_route(route: RouteId, t1: Timeish, t2: Timeish):
#     tt = get_route_timetable(route)
#     return df_time_bound(tt, t1, t2)

stop_times: pd.DataFrame = gtfs.feed.stop_times
stop_times["arrival_time"] = pd.to_timedelta(stop_times["arrival_time"])
stop_times["departure_time"] = pd.to_timedelta(stop_times["departure_time"])

def get_future_stops_on_trip(trip: TripId, stop_seq: StopSeq = 0):
    st = stop_times[stop_times["trip_id"] == str(trip)]
    return st[st["stop_sequence"] > int(stop_seq)]

visited_stops: dict[str, datetime] = dict() # stop_id, first time we reach stop
visited_trips: set[str] = set()

end_timedelta = dt_minus_date(end_time, START_TIME.date())

queue = [(timedelta_coerce(START_TIME.time()), str(START_STOP))] # time we get there, stop id
heapq.heapify(queue)
t = tqdm()
while len(queue):
    t.update()
    td, stop_id = heapq.heappop(queue)
    if stop_id in visited_stops:
        continue
    visited_stops[stop_id] = datetime.combine(START_TIME.date(), time()) + td
    stop_timetable = trips_between_for_stop(stop_id, td, end_timedelta)
    first_available_routes = stop_timetable.drop_duplicates('route_id', keep='first')
    # print(stop_timetable)
    for _, row in first_available_routes.iterrows():
        trip_id, stop_seq = row["trip_id"], row["stop_sequence"]
        if trip_id in visited_trips:
            continue
        visited_trips.add(trip_id)
        # print(stop_id, row)
        for _, future_stop in get_future_stops_on_trip(trip_id, stop_seq).iterrows():
            heapq.heappush(queue, (timedelta_coerce(future_stop["arrival_time"]), future_stop["stop_id"]))

print(f'Evaluated {len(visited_trips)} trips and found {len(visited_stops)} reachable stops.')

# import pprint
# pprint.pprint(visited_stops)

m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

for stop_id, dt in visited_stops.items():
    stop = gtfs.feed.stops[gtfs.feed.stops["stop_id"] == stop_id].iloc[0]
    name, lat, lon = stop["stop_desc"], stop["stop_lat"], stop["stop_lon"]
    folium.Circle(
        location=[lat, lon],
        tooltip=name,
        popup=f'{name}\nEarliest time: {dt.strftime("%m/%d %H:%M:%S")}',
        fill_color="#f00",
        fill_opacity=0.2,
        color="black",
        weight=1,
        radius=804.672,
    ).add_to(m)

m.save("jetlag.html")

from collections import defaultdict
import functools
import heapq

import pandas as pd
from tqdm import tqdm
from gtfslib import GTFS, CoordsUtil
from pathlib import Path
import folium
from datetime import datetime, timedelta, date, time

import requests
import shutil

START_TIME = datetime(2024, 12, 16, 9, 0, 0)
HIDE_DURATION = timedelta(minutes=30)
START_STOP = 22750 # Akard
WALKING_SPEED = 1.06 # m/s

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

visited_stops: dict[str, tuple[datetime, tuple[str]]] = dict() # stop_id : first time we reach stop, fastest route combo
visited_trips: set[str] = set()

added_stops: dict[str, timedelta] = dict() # temp dict to stop adding to queue

end_timedelta = dt_minus_date(end_time, START_TIME.date())


queue = [(timedelta_coerce(START_TIME.time()), str(START_STOP), ())] # time we get there, stop id, tuple with ordered route names
heapq.heapify(queue)

def push_to_queue(arrival_time: Timeish, stop_id: StopId, routes: tuple[str]):
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
    for _, row in first_available_routes.iterrows():
        trip_id, stop_seq, trip_name = row["trip_id"], row["stop_sequence"], row["trip_headsign"]
        if trip_id in visited_trips:
            continue
        visited_trips.add(trip_id)
        # print(stop_id, row)
        for _, future_stop in get_future_stops_on_trip(trip_id, stop_seq).iterrows():
            arrival_time = timedelta_coerce(future_stop["arrival_time"])
            if arrival_time > end_timedelta:
                continue
            future_stop_id = future_stop["stop_id"]
            push_to_queue(arrival_time, future_stop_id, routes + (trip_name,))

    # if we had just walked, walking again is not going to provide new stations
    if len(routes) and routes[-1] == "walking":
        continue

    # walking calculation
    remaining_time = end_timedelta - td
    walking_distance = WALKING_SPEED * remaining_time.seconds
    stop_df = gtfs.get_stop(stop_id)
    lat, lon = stop_df["stop_lat"], stop_df["stop_lon"]
    buffered_area = CoordsUtil.buffer_points(walking_distance, (lat, lon))
    stops_in_area = gtfs.feed.get_stops_in_area(buffered_area)
    for _, row in stops_in_area.iterrows():
        distance_to_stop = CoordsUtil.coord_distance((lat, lon), (row["stop_lat"], row["stop_lon"]))
        arrival_time = td + (distance_to_stop / WALKING_SPEED * timedelta(seconds=1))
        future_stop_id = row["stop_id"]
        push_to_queue(arrival_time, future_stop_id, routes + ("walking",))

t.close()

print(f'Evaluated {len(visited_trips)} trips and found {len(visited_stops)} reachable stops.')

# import pprint
# pprint.pprint(visited_stops)

m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

for stop_id, (dt, routes) in visited_stops.items():
    stop = gtfs.get_stop(stop_id)
    name, lat, lon = stop["stop_name"], stop["stop_lat"], stop["stop_lon"]
    is_rail_station = "station" in name.lower()
    route_text = []
    if not len(routes):
        route_text.append('We started here!')
    else:
        route_text += [ ' - '+route for route in routes ]

    popup_lines = [
        name,
        f'Earliest time: {dt.strftime("%m/%d %H:%M:%S")}',
        '',
        'Steps:'
    ]
    popup_lines += route_text

    folium.Circle(
        location=[lat, lon],
        tooltip=name,
        popup='<br>'.join(popup_lines),
        fill_color="#00f" if is_rail_station else "#f00",
        fill_opacity=0.2,
        color="black",
        weight=1,
        radius=804.672 if is_rail_station else 20,
    ).add_to(m)

m.save("jetlag.html")

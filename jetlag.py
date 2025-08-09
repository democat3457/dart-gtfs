from __future__ import annotations

from dataclasses import dataclass
import functools
import heapq
import itertools
from operator import itemgetter

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


from flask import Flask, render_template, request
app = Flask(__name__)


DEFAULT_START_TIME = datetime(2025, 1, 20, 9, 0, 0)
DEFAULT_HIDE_DURATION = timedelta(minutes=90)
DEFAULT_START_STOP = 22750  # Akard
DEFAULT_WALKING_SPEED = 1.06  # m/s
DEFAULT_ALLOWED_TRAVEL_MODES = RouteType.all()
DEFAULT_ALLOWED_HIDING_MODES = [RouteType.LIGHT_RAIL]

_default_allowed_travel_modes = ','.join(mode.name for mode in DEFAULT_ALLOWED_TRAVEL_MODES)
_default_allowed_hiding_modes = ','.join(mode.name for mode in DEFAULT_ALLOWED_HIDING_MODES)

data_folder = Path("data")
if not data_folder.exists():
    data_folder.mkdir()
export_folder = Path("export")
if not export_folder.exists():
    export_folder.mkdir()

# 0=debug, 1=info, 2=warn, 3=error
VERBOSITY = 1

def loginfo(*args, **kwargs):
    if VERBOSITY <= 1:
        print(*args, **kwargs)

def logwarn(*args, **kwargs):
    if VERBOSITY <= 2:
        print(*args, **kwargs)


StopId = str | int
TripId = str | int
StopSeq = int
Timeish = time | timedelta


def timedelta_coerce(t: Timeish):
    if isinstance(t, (timedelta, pd.Timedelta)):
        return t
    return datetime.combine(date.min, t) - datetime.combine(date.min, time())

def timeish_hms_colon_str(t: Timeish):
    td_sec = timedelta_coerce(t).seconds
    h, m, s = td_sec // 3600, (td_sec % 3600) // 60, td_sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def timeish_minsec_str(t: Timeish):
    td_sec = timedelta_coerce(t).seconds
    m, s = td_sec // 60, td_sec % 60
    return f"{m}m{s:02d}s"

def dt_minus_date(dt: datetime, d: date):
    return dt - datetime.combine(d, time())

def df_time_bound(
    df: pd.DataFrame, lower: Timeish | None = None, upper: Timeish | None = None
):
    mask = pd.notna(df["departure_time"]) & pd.notna(df["arrival_time"])
    if lower:
        mask &= df["departure_time"] >= timedelta_coerce(lower)
    if upper:
        mask &= df["arrival_time"] <= timedelta_coerce(upper)
    return df[mask]

class RouteSegmentCollection:
    @dataclass
    class RouteSegment:
        departure_td: timedelta
        arrival_td: timedelta
        route_name: str
        arrival_stop_id: StopId

    # TODO use special route segment flags rather than checking route names
    STARTING_ROUTE_NAME = "__start__"

    def __init__(self, day: date, *trips: RouteSegment):
        self.day = day
        self.trips = trips

    def append(
        self,
        departure_td: timedelta,
        arrival_td: timedelta,
        route_name: str,
        arrival_stop_id: StopId,
    ) -> RouteSegmentCollection:
        return self.append_(
            RouteSegmentCollection.RouteSegment(
                departure_td, arrival_td, route_name, arrival_stop_id
            )
        )

    def append_(self, trip: RouteSegment) -> RouteSegmentCollection:
        return RouteSegmentCollection(self.day, *self.trips, trip)

    def get_last_trip(self) -> RouteSegment | None:
        if not len(self.trips):
            return None
        return self.trips[-1]

    def get_arrival_dt(self) -> datetime | None:
        if (last_trip := self.get_last_trip()) is None:
            return None
        return datetime.combine(self.day, time()) + last_trip.arrival_td

    def populate_waiting(self) -> RouteSegmentCollection:
        segments = [self.trips[0]]
        for a, b in itertools.pairwise(self.trips):
            if a.arrival_td != b.departure_td:
                wait_route_name = f"Wait at stop"
                segments.append(
                    RouteSegmentCollection.RouteSegment(
                        a.arrival_td, b.departure_td, wait_route_name, a.arrival_stop_id
                    )
                )
            segments.append(b)
        return RouteSegmentCollection(self.day, *segments)

    def to_str(self, sep: str = "\n") -> list[str]:
        route_text = [
            f"{gtfs.stop_names[str(self.get_last_trip().arrival_stop_id)]}",
            f'Arrival time: {self.get_arrival_dt().strftime("%m/%d %H:%M:%S")}',
            "",
            "Steps:",
        ]
        for segment in self.trips:
            arrival_stop_name = gtfs.stop_names[str(segment.arrival_stop_id)]
            if segment.route_name == self.__class__.STARTING_ROUTE_NAME:
                route_text.append(
                    f"{timeish_hms_colon_str(segment.departure_td)} Start at {arrival_stop_name}"
                )
            else:
                route_str = segment.route_name
                if not (route_str.startswith("Walk ") or route_str.startswith("Wait ")):
                    route_str = "Take " + route_str
                route_text.append(
                    f" - ({timeish_minsec_str(segment.arrival_td - segment.departure_td)}) {route_str}"
                )
                route_text.append(
                    f"{timeish_hms_colon_str(segment.arrival_td)} {arrival_stop_name}"
                )
        return sep.join(route_text)

    @classmethod
    def starting_collection(cls, start_dt: datetime, start_stop_id: StopId):
        td = timedelta_coerce(start_dt.time())
        return cls(start_dt.date()).append(
            td, td, cls.STARTING_ROUTE_NAME, start_stop_id
        )

    def __str__(self):
        return str(self.trips)

    def __iter__(self):
        return iter(self.trips)

    def __len__(self):
        return len(self.trips)

    def __get_cmp_key(self):
        if not len(self.trips):
            raise ValueError("route collection needs a segment to compare against")
        return (self.get_last_trip().arrival_td, len(self.trips))

    def __lt__(self, other):
        if not isinstance(other, RouteSegmentCollection):
            raise TypeError
        return self.__get_cmp_key() < other.__get_cmp_key()

    def __gt__(self, other):
        if not isinstance(other, RouteSegmentCollection):
            raise TypeError
        return self.__get_cmp_key() > other.__get_cmp_key()

    def __hash__(self):
        return hash(self.trips)

    def __eq__(self, other):
        return isinstance(other, RouteSegmentCollection) and self.trips == other.trips


def init_gtfs(filename: str, url: str):
    file = Path(data_folder) / filename

    loginfo("Initializing GTFS...")
    _init_time = pytime.time()

    download_file = True
    if file.exists():
        _gtfs = GTFS(file)
        if _gtfs.start_date <= date.today() <= _gtfs.end_date:
            download_file = False

    if download_file:
        loginfo("Downloading DART GTFS zip file...")
        r = requests.get(
            url, stream=True
        )
        if r.status_code == 200:
            with file.open("wb") as out_file:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, out_file)
        _gtfs = GTFS(file)

    # Access properties to cache elements
    _gtfs.stop_route_types
    _gtfs.stop_names

    _init_time_stop = pytime.time()
    loginfo(f"Finished initialization in {_init_time_stop-_init_time:.2f}s.")

    return _gtfs


gtfs = init_gtfs(
    "dart_gtfs.zip", "https://www.dart.org/transitdata/latest/google_transit.zip"
)

stop_times: pd.DataFrame = gtfs.feed.stop_times
stop_times["arrival_time"] = pd.to_timedelta(stop_times["arrival_time"])
stop_times["departure_time"] = pd.to_timedelta(stop_times["departure_time"])
stop_times_by_trip = stop_times.groupby(["trip_id"], sort=False)

def get_future_stops_on_trip(trip: TripId, stop_seq: StopSeq = 0):
    st = stop_times_by_trip.get_group((str(trip),))
    return st[st["stop_sequence"] > int(stop_seq)]

@functools.lru_cache()
def get_stop_timetable(stop: StopId, day: str):
    tt = gtfs.build_stop_timetable(str(stop), [day])
    tt["arrival_time"] = pd.to_timedelta(tt["arrival_time"])
    tt["departure_time"] = pd.to_timedelta(tt["departure_time"])
    return tt

def trips_between_for_stop(stop: StopId, day: str, t1: Timeish, t2: Timeish):
    tt = get_stop_timetable(stop, day)
    return df_time_bound(tt, t1, t2)


def get_starting_stops():
    # ALLOWED_HIDING_MODES = [ RouteType[route_type] for route_type in data.get('hiding_modes', _default_allowed_hiding_modes).split(',') ]
    ALLOWED_HIDING_MODES = [ RouteType[route_type] for route_type in (_default_allowed_hiding_modes).split(',') ]
    return sorted(
        filter(
            lambda stop_info: any(
                rtype in ALLOWED_HIDING_MODES
                for rtype in gtfs.stop_route_types[stop_info[0]]
            ),
            gtfs.stop_names.items(),
        ),
        key=itemgetter(1),
    )


@app.route("/")
def index():
    dt_start = datetime.combine(gtfs.start_date, time(0,0))
    dt_end = datetime.combine(gtfs.end_date, time(23, 59))
    dt_now = max(datetime.now(), dt_start).replace(second=0, microsecond=0)
    return render_template("jetlag.html",
                           starting_stop_list=get_starting_stops(),
                           now_time=dt_now.isoformat(),
                           start_date=dt_start.isoformat(),
                           end_date=dt_end.isoformat())


@app.route("/jetlag-map", methods=['POST'])
def jetlag_map():
    data = request.form
    START_TIME = datetime.fromisoformat(data.get('start_time', DEFAULT_START_TIME.isoformat()))
    _hide_duration = int(data.get('hide_duration_minutes', DEFAULT_HIDE_DURATION.seconds // 60))
    _end_time = data.get('end_time', None)
    END_TIME = datetime.fromisoformat(_end_time) if _end_time else START_TIME + timedelta(minutes=_hide_duration)
    START_STOP = data.get('start_stop_id', DEFAULT_START_STOP)
    WALKING_SPEED = float(data.get('walking_speed', DEFAULT_WALKING_SPEED))
    ALLOWED_TRAVEL_MODES = [ RouteType[route_type] for route_type in data.get('travel_modes', _default_allowed_travel_modes).split(',') ]
    ALLOWED_HIDING_MODES = [ RouteType[route_type] for route_type in data.get('hiding_modes', _default_allowed_hiding_modes).split(',') ]

    if not (gtfs.start_date <= START_TIME.date() <= gtfs.end_date):
        return "<strong>Start time not in GTFS feed range!</strong>"
    if not (gtfs.start_date <= END_TIME.date() <= gtfs.end_date):
        return "<strong>End time not in GTFS feed range!</strong>"
    if not (gtfs.start_date < gtfs.end_date):
        return "<strong>End time must be after start time!</strong>"

    print(data)
    print(START_TIME, END_TIME, START_STOP, WALKING_SPEED)

    today_str = START_TIME.strftime("%Y%m%d")

    visited_stops: dict[str, RouteSegmentCollection] = dict() # stop_id : fastest route combo
    visited_trips: set[str] = set()

    added_stops: dict[str, timedelta] = dict() # temp dict to stop adding to queue

    end_timedelta = dt_minus_date(END_TIME, START_TIME.date())

    queue = [RouteSegmentCollection.starting_collection(START_TIME, str(START_STOP))]
    heapq.heapify(queue)

    def push_to_queue(route_collection: RouteSegmentCollection):
        stop_id, arrival_time = route_collection.get_last_trip().arrival_stop_id, route_collection.get_last_trip().arrival_td
        if stop_id in added_stops:
            if arrival_time > added_stops[stop_id]:
                # if stop has already been added and the tentative time is later than the already queued time, skip
                return
        heapq.heappush(queue, route_collection)
        added_stops[stop_id] = arrival_time

    t = tqdm()
    while len(queue):
        t.set_description(str(len(queue)), refresh=False)
        t.update()
        route_collection = heapq.heappop(queue)
        td, stop_id = route_collection.get_last_trip().arrival_td, route_collection.get_last_trip().arrival_stop_id
        if stop_id in visited_stops:
            continue
        if td > end_timedelta:
            continue
        visited_stops[stop_id] = route_collection
        stop_timetable = trips_between_for_stop(stop_id, today_str, td, end_timedelta)
        first_available_routes = stop_timetable.drop_duplicates(('route_id', 'direction_id'), keep='first')
        # print(stop_timetable)
        for _, row_gdf in first_available_routes.iterrows():
            trip_id, stop_seq, trip_name = row_gdf["trip_id"], row_gdf["stop_sequence"], row_gdf["trip_headsign"]
            departure_time = timedelta_coerce(row_gdf["departure_time"])
            if pd.isna(trip_name):
                # extrapolate trip route name
                rt_short_name = gtfs._routes_by_id.at[row_gdf["route_id"], "route_short_name"]
                trip_name = f"{rt_short_name} (NO DEST)"

            # only travel in allowed route types
            if gtfs.trip_route_types[trip_id] not in ALLOWED_TRAVEL_MODES:
                continue

            if trip_id in visited_trips:
                continue
            visited_trips.add(trip_id)

            for _, future_stop in get_future_stops_on_trip(trip_id, stop_seq).iterrows():
                arrival_time = timedelta_coerce(future_stop["arrival_time"])
                if arrival_time > end_timedelta:
                    continue
                future_stop_id = future_stop["stop_id"]
                push_to_queue(route_collection.append(departure_time, arrival_time, trip_name, future_stop_id))

        # if we had just walked, walking again is not going to provide new stations
        if route_collection.get_last_trip().route_name.startswith("Walk "):
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
            distance_to_stop_miles = distance_to_stop / 1609.34
            arrival_time = td + (distance_to_stop / WALKING_SPEED * timedelta(seconds=1))
            future_stop_id = row["stop_id"]
            push_to_queue(route_collection.append(td, arrival_time, f"Walk {distance_to_stop_miles:.2f} miles ({round(distance_to_stop)} m)", future_stop_id))

    t.close()

    print(f'Evaluated {len(visited_trips)} trips and found {len(visited_stops)} reachable stops.')

    # import pprint
    # pprint.pprint(visited_stops)

    m = folium.Map(location=[32.7769, -96.7972], zoom_start=10)

    for stop_id, route_collection in visited_stops.items():
        stop = gtfs.get_stop(stop_id).to_crs(Projections.WGS84).iloc[0]
        name, point = stop["stop_name"], stop.geometry
        lon, lat = point.x, point.y
        is_valid_hiding_spot = any(
            rtype in ALLOWED_HIDING_MODES
            for rtype in gtfs.stop_route_types[stop_id]
        )

        popup = folium.Popup(
            route_collection.populate_waiting().to_str(sep='<br>'),
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

    html = m.get_root().render()
    return html

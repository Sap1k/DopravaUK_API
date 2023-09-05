import datetime
from typing import Union, List, Dict, Tuple

import mysql.connector
import pytz
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
# from geopy.distance import geodesic
from pydantic import BaseModel


class GetVhcPos(BaseModel):
    vhc_id: Union[int, None] = None
    line_displayed: Union[str, None] = None
    trip: Union[int, None] = None


class GetVhcInfoByID(BaseModel):
    ID: int


class GetVhcInfoByTrip(BaseModel):
    line_displayed: str
    trip: int


class ReturnVhcInfo(BaseModel):
    vhc_id: int
    on_trip: bool
    line_displayed: str
    trip: int
    is_train: bool
    end_stop: str
    current_stop: str
    delay: int
    agency: str
    accessible: bool
    last_ping: str


class ReturnVhcInfoList(BaseModel):
    __root__: List[ReturnVhcInfo]


class ReturnVhcDetails(BaseModel):
    model: Union[str, None] = None
    agency: str
    year_of_manufacture: Union[int, None] = None
    accessible: bool
    contactless_payments: bool
    air_conditioning: bool
    alternate_fuel: bool
    usb_chargers: bool


class ReturnVhcPos(BaseModel):
    vhc_id: int
    lat: float
    lng: float
    azimuth: int


class ReturnVhcPosList(BaseModel):
    __root__: List[ReturnVhcPos]


class ReturnStop(BaseModel):
    stop_name: str
    stop_lat: float
    stop_lng: float
    zone_id: str
    wheelchair_boarding: bool


class ReturnStopList(BaseModel):
    __root__: List[ReturnStop]


class ReturnStopOnTrip(BaseModel):
    stop_name: str
    arrival_time: datetime.time
    departure_time: datetime.time
    wheelchair_boarding: bool


class ReturnStopsOnTrip(BaseModel):
    line: str
    trip: int
    last_stop: str
    agency: str
    accessible: bool
    stops: List[ReturnStopOnTrip]


class GetDepartures(BaseModel):
    stop: str


class ReturnStaticDeparture(BaseModel):
    line: str
    trip: int
    agency: str
    last_stop: str
    planned_departure: str
    low_floor: bool


class ReturnRTDeparture(BaseModel):
    line: str
    trip: int
    agency: str
    last_stop: str
    rt_available: bool
    delay: int
    planned_departure: str
    low_floor: bool


class ReturnStaticDepartureList(BaseModel):
    __root__: List[ReturnStaticDeparture]


class ReturnRTDepartureList(BaseModel):
    __root__: List[ReturnRTDeparture]


def get_con(db_name):
    db = mysql.connector.connect(
        host="#",
        user="#",
        password="#",
        database=db_name,
        buffered=True)
    db.start_transaction(isolation_level='READ COMMITTED')
    return db


def get_correct_time(time):
    if time[:2] == '24':
        days_to_add = 1
        departure_time = '00' + time[2:]
    else:
        days_to_add = 0
        departure_time = time

    return days_to_add, departure_time


# def get_con(db_name):
#     db = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         # password="jr",
#         database=db_name,
#         buffered=True)
#     db.start_transaction(isolation_level='READ COMMITTED')
#     return db


async def get_vhc_data_new(vhc_id=None, line=None, trip=None):
    con_del = get_con("buses_duk")
    cur_del = con_del.cursor()
    con_jr = get_con("DUK_JR")
    cur_jr = con_jr.cursor()

    if line and trip:
        cur_del.execute("SELECT vhc_id, line, trip, last_stop_id, delay, last_changed FROM delays "
                        "WHERE line = %s AND trip = %s ORDER BY last_changed DESC LIMIT 1", (line, trip))
    elif vhc_id:
        cur_del.execute("SELECT vhc_id, line, trip, last_stop_id, delay, last_changed FROM delays "
                        "WHERE vhc_id = %s ORDER BY last_changed DESC LIMIT 1", (vhc_id,))
    else:
        return None

    vhc_details = cur_del.fetchone()

    if not vhc_details:
        return None

    sql_gtfs = "SELECT stops.stop_name AS current_stop, stop_times.departure_time, trips.wheelchair_accessible, agency.agency_name," \
               "(SELECT stops.stop_name FROM stops JOIN stop_times ON stops.stop_id = stop_times.stop_id WHERE stop_times.trip_id = trips.trip_id ORDER BY stop_times.stop_sequence DESC LIMIT 1) AS last_stop " \
               "FROM trips " \
               "JOIN routes ON routes.route_id=trips.route_id " \
               "JOIN agency ON agency.agency_id=routes.agency_id " \
               "JOIN stop_times ON stop_times.trip_id=trips.trip_id  " \
               "JOIN stops ON stop_times.stop_id=stops.stop_id " \
               "WHERE trips.trip_short_name LIKE %s " \
               "AND stops.stop_id = %s " \
               "ORDER BY stop_sequence"

    linetrip = f"%{vhc_details[1]} {vhc_details[2]}"
    last_stop_id = vhc_details[3]
    cur_jr.execute(sql_gtfs, (linetrip, last_stop_id))
    gtfs_details = cur_jr.fetchone()

    if not gtfs_details:
        gtfs_details = ('?', '?', 1, '?', '?')

    if (datetime.datetime.now() - vhc_details[5]).total_seconds() > 360:
        on_trip = False
    elif gtfs_details[0] == gtfs_details[4]:
        on_trip = False
    else:
        on_trip = True

    resp = {
        "vhc_id": vhc_details[0],
        "on_trip": on_trip,
        "line_displayed": vhc_details[1],
        "trip": vhc_details[2],
        "end_stop": gtfs_details[4],
        "current_stop": gtfs_details[0],
        "delay": int(vhc_details[4] // 60),
        "agency": gtfs_details[3],
        "accessible": bool(gtfs_details[2]),
        "last_ping": str(vhc_details[5])
    }

    cur_del.close()
    cur_jr.close()
    con_del.close()
    con_jr.close()

    return resp


app = FastAPI()

origins = [
    "http://127.0.0.1:5000",
    "https://dukfinder.sap1k.cz"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/GetVhcPos", response_model=ReturnVhcPosList)
async def pozice_spoju(request: GetVhcPos):
    con = get_con("buses_duk")
    cur = con.cursor()

    if request.line_displayed and request.trip:
        cur.execute("SELECT DISTINCT vhc_id, vhc_lat, vhc_lon, vhc_azimuth FROM delays WHERE line = %s AND trip = %s "
                    "ORDER BY last_changed DESC", (request.line_displayed, request.trip))
        data_markers = cur.fetchall()
    elif request.vhc_id:
        cur.execute("SELECT DISTINCT vhc_id, vhc_lat, vhc_lon, vhc_azimuth FROM delays WHERE vhc_id = %s "
                    "ORDER BY last_changed DESC", (request.vhc_id,))
        data_markers = cur.fetchall()
    else:
        cur.execute("SELECT DISTINCT vhc_id, vhc_lat, vhc_lon, vhc_azimuth FROM delays ORDER BY last_changed DESC")
        data_markers = cur.fetchall()

    cur.close()
    con.close()

    res_list = [{
        "vhc_id": vhc[0],
        "lat": vhc[1],
        "lng": vhc[2],
        "azimuth": vhc[3]
    } for vhc in data_markers]

    return res_list


@app.post("/GetVhcInfoByTrip", response_model=ReturnVhcInfo)
async def data_o_spoji(request: GetVhcInfoByTrip):
    vhc_data = await get_vhc_data_new(line=request.line_displayed, trip=request.trip)

    if vhc_data is None:
        raise HTTPException(status_code=404, detail="linetrip not found in database!")

    if len(str(vhc_data["vhc_id"])) == 5:
        is_train = True
    else:
        is_train = False

    return {
        "vhc_id": vhc_data["vhc_id"],
        "on_trip": vhc_data["on_trip"],
        "line_displayed": vhc_data["line_displayed"],
        "trip": vhc_data["trip"],
        "is_train": is_train,
        "end_stop": vhc_data["end_stop"],
        "current_stop": vhc_data["current_stop"],
        "delay": vhc_data["delay"],
        "agency": vhc_data["agency"],
        "accessible": vhc_data["accessible"],
        "last_ping": vhc_data["last_ping"]
    }


@app.post("/GetVhcInfoByID", response_model=ReturnVhcInfo)
async def data_o_vozu(request: GetVhcInfoByID):
    vhc_data = await get_vhc_data_new(vhc_id=request.ID)

    if vhc_data is None:
        raise HTTPException(status_code=404, detail="vhc_id not found in database!")

    if len(str(vhc_data["vhc_id"])) == 5:
        is_train = True
    else:
        is_train = False

    return {
        "vhc_id": vhc_data["vhc_id"],
        "on_trip": vhc_data["on_trip"],
        "line_displayed": vhc_data["line_displayed"],
        "trip": vhc_data["trip"],
        "is_train": is_train,
        "end_stop": vhc_data["end_stop"],
        "current_stop": vhc_data["current_stop"],
        "delay": vhc_data["delay"],
        "agency": vhc_data["agency"],
        "accessible": vhc_data["accessible"],
        "last_ping": vhc_data["last_ping"]
    }


@app.post("/GetVhcDetailsByID", response_model=ReturnVhcDetails)
async def detaily_o_vozu(request: GetVhcInfoByID):
    con = get_con("buses_duk")
    cur = con.cursor()
    cur.execute(f'SELECT * FROM vehicles WHERE vhc_id = {request.ID}')
    if cur.rowcount == 0:
        cleandata = await get_vhc_data_new(request.ID)
        # Vehicle not in our db, return expected values for DÚK (according to quality requirements for all contracts
        # from 2014 onwards (basically all valid contracts except trains, which will get their own system based on
        # line/trip later
        model = None
        agency = cleandata["agency"]
        year_of_manufacture = None
        accessible = cleandata["accessible"]
        contactless_payments = False
        air_conditioning = True
        alternate_fuel = False
        usb_chargers = False
    else:
        vhc_details = cur.fetchone()
        model = vhc_details[1]
        agency = vhc_details[2]
        year_of_manufacture = int(vhc_details[3])
        accessible = bool(vhc_details[4])
        contactless_payments = bool(vhc_details[5])
        air_conditioning = bool(vhc_details[6])
        alternate_fuel = bool(vhc_details[7])
        usb_chargers = bool(vhc_details[8])

    cur.close()
    con.close()

    return {
        "model": model,
        "agency": agency,
        "year_of_manufacture": year_of_manufacture,
        "accessible": accessible,
        "contactless_payments": contactless_payments,
        "air_conditioning": air_conditioning,
        "alternate_fuel": alternate_fuel,
        "usb_chargers": usb_chargers
    }


@app.get("/GetStops", response_model=ReturnStopList)
async def data_zastavek():
    con = get_con("DUK_JR")
    cur = con.cursor()
    cur.execute("SELECT stop_name, stop_lat, stop_lon, zone_id, wheelchair_boarding FROM stops ORDER BY stop_name")
    source_stops = cur.fetchall()
    all_stops = list()
    cur.close()
    con.close()
    for stop in source_stops:
        stop_clean = {
            'stop_name': stop[0],
            'stop_lat': stop[1],
            'stop_lng': stop[2],
            'zone_id': stop[3],
            'wheelchair_boarding': bool(int(stop[4])),
        }
        all_stops.append(stop_clean)

    return all_stops


@app.post("/GetStopsOnTrip", response_model=ReturnStopsOnTrip)
async def trasa_spoje(request: GetVhcInfoByTrip):
    con = get_con("DUK_JR")
    cur = con.cursor()

    line = request.line_displayed
    trip = request.trip

    busquery = "SELECT DISTINCT agency.agency_name, trips.wheelchair_accessible, " \
               "(SELECT stops.stop_name FROM stops JOIN stop_times ON stops.stop_id = stop_times.stop_id WHERE " \
               "stop_times.trip_id = trips.trip_id ORDER BY stop_times.stop_sequence DESC LIMIT 1) AS last_stop " \
               "FROM trips " \
               "JOIN routes ON routes.route_id=trips.route_id " \
               "JOIN agency ON agency.agency_id=routes.agency_id " \
               "JOIN stop_times ON stop_times.trip_id=trips.trip_id " \
               "JOIN stops ON stop_times.stop_id=stops.stop_id " \
               "WHERE trips.trip_short_name LIKE %s "

    tripquery = "SELECT stops.stop_name, stop_times.arrival_time, stop_times.departure_time, " \
                "stops.wheelchair_boarding " \
                "FROM trips " \
                "JOIN routes ON routes.route_id=trips.route_id " \
                "JOIN agency ON agency.agency_id=routes.agency_id " \
                "JOIN stop_times ON stop_times.trip_id=trips.trip_id " \
                "JOIN stops ON stop_times.stop_id=stops.stop_id " \
                "WHERE trips.trip_short_name LIKE %s " \
                "ORDER BY stop_sequence"

    cur.execute(tripquery, (f"%{line} {trip}",))
    res_stops_raw = cur.fetchall()

    res_stops = [{
        "stop_name": stop[0],
        "arrival_time": get_correct_time(stop[1])[1],
        "departure_time": get_correct_time(stop[2])[1],
        "wheelchair_boarding": bool(stop[3])
    } for stop in res_stops_raw]

    cur.execute(busquery, (f"%{line} {trip}",))
    res_bus = cur.fetchone()

    if not res_stops or not res_bus:
        raise HTTPException(status_code=404, detail="linetrip not found in database!")

    resp = {
        'line': line,
        'trip': trip,
        'last_stop': res_bus[2],
        'agency': res_bus[0],
        'accessible': res_bus[1],
        'stops': res_stops
    }

    return resp


@app.post("/GetStaticDepartures", response_model=ReturnStaticDepartureList)
async def odjezdy(request: GetDepartures):
    stop_name = request.stop
    stop_deps = await get_db_departures(stop_name)

    dep_list = list()
    for departure in stop_deps:
        linetrip = departure[2]
        trip = linetrip[7:]
        line = linetrip[3:6]

        time_to_use = get_correct_time(departure[3])

        dep_list.append({
            'line': str(line),
            'trip': trip,
            'agency': departure[4],
            'last_stop': departure[5],
            'planned_departure': time_to_use[1],
            'low_floor': bool(departure[6])
        })

    return dep_list


@app.post("/GetRTDepartures", response_model=ReturnRTDepartureList)
async def rt_odjezdy(request: GetDepartures):
    con = get_con("buses_duk")
    cur = con.cursor()
    # For making all our datetimes TZ-aware
    timezone = pytz.timezone('Europe/Prague')
    # time = timezone.localize(datetime.datetime.combine(datetime.datetime.today(), datetime.time(hour=6, minute=10)))
    time = datetime.datetime.now(tz=timezone)
    # Fetch all static departures to be messed with later
    # --If going back by an hour would span midnight, don't
    if (time - datetime.timedelta(minutes=60)) == datetime.date.today():
        static_deps = await get_db_departures(request.stop, time - datetime.timedelta(minutes=60))
    else:
        static_deps = await get_db_departures(request.stop, time)
    dep_list = list()

    # Fetch all vehicles that have pinged their location in the last three minutes
    cur.execute("SELECT line, trip FROM delays WHERE last_changed > %s", (time - datetime.timedelta(minutes=3),))
    current_vhcs = cur.fetchall()

    for departure in static_deps:
        linetrip_str = departure[2]
        trip = int(linetrip_str[7:])
        line = linetrip_str[3:6]

        linetrip_tup = (line, trip)

        # Check only against recently pinged vehicles - this greatly improves perfomance (around 2-2.5x) and DB load
        if linetrip_tup in current_vhcs:
            rt_data_array = await get_vhc_data_new(line=line, trip=trip)
        else:
            rt_data_array = None

        # If no data array is present or vehicle isn't on trip, set sane defaults
        if not rt_data_array:
            rt_data_array = {"on_trip": False,
                             "delay": 0}
        elif rt_data_array["on_trip"] is False:
            rt_data_array["delay"] = 0

        rt_delay = rt_data_array["delay"]
        rt_available = rt_data_array["on_trip"]

        time_to_use = get_correct_time(departure[3])

        # Calculate real departure time including delay and add to departure list if applicable
        plan_datetime = datetime.datetime.combine(datetime.datetime.today() + datetime.timedelta(days=time_to_use[0]),
                                                  datetime.datetime.strptime(time_to_use[1], '%H:%M:%S').time())
        actual_datetime = timezone.localize(plan_datetime + datetime.timedelta(minutes=rt_delay + 1))
        if actual_datetime > time:
            dep_list.append({
                'line': str(line),
                'trip': trip,
                'agency': departure[4],
                'last_stop': departure[5],
                'planned_departure': time_to_use[1],
                'rt_available': rt_available,
                'delay': rt_delay,
                'low_floor': bool(departure[6])
            })

    return dep_list


async def get_db_departures(stop_name, time=None):
    timezone = pytz.timezone('Europe/Prague')
    if time is None:
        time = datetime.datetime.now(tz=timezone)

    con = get_con("DUK_JR")
    cur = con.cursor()
    cur.execute('SELECT stop_id FROM stops WHERE stop_name = %s', (stop_name,))
    stop = cur.fetchone()
    if not stop:
        raise HTTPException(status_code=404, detail="stop not found in database!")
    stop_id = stop[0]

    date_gtfs = time.strftime('%Y%m%d')
    date_db = time.strftime('%Y-%m-%d')
    time_gtfs = time.strftime('%H:%M:%S')

    depquery = ("SELECT DISTINCT trips.route_id, trips.trip_id, trips.trip_short_name, stop_times.departure_time, "
                "agency.agency_name, "
                "    (SELECT stops.stop_name FROM stops JOIN stop_times ON stops.stop_id = stop_times.stop_id WHERE "
                "stop_times.trip_id = trips.trip_id ORDER BY stop_times.stop_sequence DESC LIMIT 1) AS last_stop,"
                "trips.wheelchair_accessible "
                "FROM calendar "
                "JOIN trips ON calendar.service_id=trips.service_id "
                "JOIN stop_times ON stop_times.trip_id=trips.trip_id "
                "JOIN routes ON routes.route_id=trips.route_id "
                "JOIN agency ON routes.agency_id=agency.agency_id "
                "WHERE %s between calendar.start_date and calendar.end_date "
                "AND stop_times.stop_id = %s "
                "AND stop_times.departure_time >= %s "
                "AND (CASE WEEKDAY(%s) "
                "        WHEN 0 THEN monday"
                "        WHEN 1 THEN tuesday"
                "        WHEN 2 THEN wednesday"
                "        WHEN 3 THEN thursday"
                "        WHEN 4 THEN friday"
                "        WHEN 5 THEN saturday"
                "        WHEN 6 THEN sunday"
                "        END) = 1 "
                "AND stop_times.stop_sequence < ("
                "    SELECT MAX(stop_sequence) "
                "    FROM stop_times "
                "    WHERE trip_id = trips.trip_id) "
                "AND NOT EXISTS (SELECT 1 FROM calendar_dates WHERE calendar_dates.date=%s "
                "AND calendar_dates.date between calendar.start_date and calendar.end_date "
                "AND calendar.service_id=calendar_dates.service_id AND calendar_dates.exception_type=2) "
                "UNION "
                "SELECT trips.route_id, trips.trip_id, trips.trip_short_name, stop_times.departure_time, "
                "agency.agency_name, "
                "    (SELECT stops.stop_name FROM stops JOIN stop_times ON stops.stop_id = stop_times.stop_id WHERE "
                "stop_times.trip_id = trips.trip_id ORDER BY stop_times.stop_sequence DESC LIMIT 1) AS last_stop, "
                "trips.wheelchair_accessible "
                "FROM calendar_dates "
                "JOIN trips ON calendar_dates.service_id=trips.service_id "
                "JOIN stop_times ON stop_times.trip_id=trips.trip_id "
                "JOIN routes ON routes.route_id=trips.route_id "
                "JOIN agency ON routes.agency_id=agency.agency_id "
                "WHERE calendar_dates.`date` = %s "
                "AND stop_times.stop_id = %s "
                "AND stop_times.departure_time >= %s "
                "AND calendar_dates.exception_type = 1 "
                "AND stop_times.stop_sequence < ("
                "    SELECT MAX(stop_sequence) "
                "    FROM stop_times "
                "    WHERE trip_id = trips.trip_id) "
                "ORDER BY departure_time ")
    cur.execute(depquery, (date_gtfs, stop_id, time_gtfs, date_db, date_gtfs, date_gtfs, stop_id, time_gtfs))
    res = cur.fetchall()
    cur.close()
    con.close()
    return res

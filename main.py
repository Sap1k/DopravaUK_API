from datetime import datetime
from typing import Union, List

import mysql.connector
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from pydantic import BaseModel


class GetVhcPos(BaseModel):
    vhc_id: Union[int, None] = None


class GetVhcInfoByID(BaseModel):
    ID: int


class GetVhcInfoByTrip(BaseModel):
    line_displayed: str
    trip: Union[int, None] = None


class ReturnVhcInfo(BaseModel):
    vhc_id: int
    on_trip: bool
    line_displayed: str
    trip: int
    is_train: bool
    end_stop: str
    current_stop: str
    delay_according_to_OIS: int
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


def init_db():
    return mysql.connector.connect(
        host="localhost",
        user="user", # Change this
        password="password", # This too
        database="buses_duk"
    )


db = init_db()


def get_cur():
    global db
    try:
        db.ping(reconnect=True, attempts=3, delay=5)
    except mysql.connector.Error as err:
        db = init_db()
    return db.cursor(buffered=True)


app = FastAPI()


@app.post("/GetVhcPos", response_model=ReturnVhcPosList)
async def pozice_spoju(request: GetVhcPos):
    url_markers = 'https://provoz.kr-ustecky.cz/TMD/API/Map/GetVhcMarkers'
    payload_markers = {
        # TODO: This may have a very negative impact on govt infra, because by my understading it forces all vehicles
        # TODO: to refresh their positions
        'Reload': 'true',
        'ShowMissingRides': 'true',
        'SifterCarrierIDs': ''
    }
    data_markers = requests.post(url_markers, json=payload_markers).json()

    res_list = list()

    for vhc in data_markers["ItemL"]:
        if request.vhc_id is None or request.vhc_id == vhc["ID"]:
            res = {
                "vhc_id": vhc["ID"],
                "lat": vhc["Lat"],
                "lng": vhc["Lng"],
                "azimuth": vhc["Azimut"]
            }
            res_list.append(res)
        else:
            pass

    return list(res_list)


@app.post("/GetVhcInfoByTrip", response_model=ReturnVhcInfoList)
async def data_o_spoji(request: GetVhcInfoByTrip):
    vhc_ids = []
    res_list = list()
    # Define data for getting VhcMarkers
    url_markers = 'https://provoz.kr-ustecky.cz/TMD/API/Map/GetVhcMarkers'
    payload_markers = {
        # TODO: This may have a very negative impact on govt infra, because by my understading it forces all vehicles
        # TODO: to refresh their positions
        'Reload': 'true',
        'ShowMissingRides': 'true',
        'SifterCarrierIDs': ''
    }
    data_markers = requests.post(url_markers, json=payload_markers).json()

    for vhc in data_markers["ItemL"]:
        if vhc["LineText"] == request.line_displayed and request.trip is None:
            vhc_ids.append(vhc["ID"])
        elif vhc["LineText"] == request.line_displayed and vhc["RouteID"] == request.trip:
            vhc_ids.append(vhc["ID"])
        else:
            pass

    for vehicle_id in vhc_ids:
        vhc_data = GetVhcInfoByID(ID=vehicle_id)
        res = await data_o_vozu(vhc_data)
        res_list.append(res)
    bus_list = ReturnVhcInfoList(__root__=res_list)
    return bus_list


@app.post("/GetVhcInfoByID", response_model=ReturnVhcInfo)
async def data_o_vozu(request: GetVhcInfoByID):
    cleandata = await get_vhc_data(request.ID)

    if len(str(cleandata["vhc_id"])) == 5:
        is_train = True
    else:
        is_train = False

    return {
        "vhc_id": cleandata["vhc_id"],
        "on_trip": cleandata["on_trip"],
        "line_displayed": cleandata["line_displayed"],
        "trip": cleandata["trip"],
        "is_train": is_train,
        "end_stop": cleandata["end_stop"],
        "current_stop": cleandata["current_stop"],
        "delay_according_to_OIS": cleandata["delay"],
        "agency": cleandata["agency"],
        "accessible": cleandata["accessible"],
        "last_ping": cleandata["last_ping"]
    }


@app.post("/GetVhcDetailsByID", response_model=ReturnVhcDetails)
async def detaily_o_vozu(request: GetVhcInfoByID):
    cur = get_cur()
    cur.execute(f'SELECT * FROM vehicles WHERE vhc_id = {request.ID}')
    if cur.rowcount == 0:
        cleandata = await get_vhc_data(request.ID)
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


async def get_vhc_data(vhc_id):
    url = 'https://provoz.kr-ustecky.cz/TMD/ItemDetails/Get'
    cur = get_cur()
    data = requests.post(url, json={'ID': vhc_id})
    data_new = BeautifulSoup(data.text, "html.parser")
    cleandata = []

    j = 0
    for string in data_new.stripped_strings:
        if string == "Spoj nedodává data online":
            j += 2
            cleandata.append(0)
        elif string == "není k dispozici":
            j += 1
            cleandata.append(0)
        elif j == 18:
            break
        elif j % 2 == 0:
            j += 1
            pass
        else:
            j += 1
            cleandata.append(string)

    if cleandata[6] == "Ano":
        accessible = True
    else:
        accessible = False

    time_since_last_ping = datetime.strptime(cleandata[8], '%H:%M:%S %d.%m.%Y') - datetime.now()
    if cleandata[1] == "0/0":
        cleandata[1] = "0"
        ontrip = False
    elif cleandata[2] == cleandata[3]:
        ontrip = False
    elif time_since_last_ping.total_seconds() > 360:
        ontrip = False
    else:
        ontrip = True

    line_displayed = str(cleandata[1][:3]).strip('/')
    trip = str(cleandata[1][3:].strip('/'))
    if trip == "":
        trip = int(0)
    else:
        trip = int(trip)

    cur.execute(f'SELECT agency FROM vehicles WHERE vhc_id = {int(cleandata[0])}')
    if cur.rowcount == 0:
        agency = cleandata[5]
    else:
        agency = cur.fetchone()[0]

    return {
        "vhc_id": int(cleandata[0]),
        "on_trip": ontrip,
        "line_displayed": line_displayed,
        "trip": trip,
        "end_stop": cleandata[2],
        "current_stop": cleandata[3],
        "delay": int(cleandata[4]),
        "agency": agency,
        "accessible": accessible,
        "last_ping": cleandata[8]
    }

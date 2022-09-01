from typing import Union, List
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from pydantic import BaseModel


class GetVhcInfoByID(BaseModel):
    ID: int


class GetVhcInfoByTrip(BaseModel):
    line_displayed: str
    trip: Union[int, None] = None


class ReturnVhcInfo(BaseModel):
    vehicle_id: int
    on_trip: bool
    line_displayed: str
    trip: int
    end_stop: str
    current_stop: str
    delay_according_to_OIS: int
    agency: str
    accessible: bool
    last_ping: str


class ReturnVhcInfoList(BaseModel):
    __root__: List[ReturnVhcInfo]


app = FastAPI()


@app.post("/GetVhcInfoByTrip", response_model=ReturnVhcInfoList)
async def data_o_spoji(request: GetVhcInfoByTrip):
    vehicle_ids = []
    res = None
    res_tuple = list()
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
    dict_vehicle_id = {}

    for vehicle in data_markers["ItemL"]:
        if vehicle["LineText"] == request.line_displayed and request.trip is None:
            vehicle_ids.append(vehicle["ID"])
        elif vehicle["LineText"] == request.line_displayed and vehicle["RouteID"] == request.trip:
            vehicle_ids.append(vehicle["ID"])
        else:
            pass

    for vehicle_id in vehicle_ids:
        vhc_data = GetVhcInfoByID(ID=vehicle_id)
        # print(res)
        res = await data_o_vozu(vhc_data)
        res_tuple.append(res)
    bus_list = ReturnVhcInfoList(__root__=res_tuple)
    return bus_list


@app.post("/GetVhcInfoByID", response_model=ReturnVhcInfo)
async def data_o_vozu(request: GetVhcInfoByID):
    url = 'https://provoz.kr-ustecky.cz/TMD/ItemDetails/Get'

    data = requests.post(url, json={'ID': request.ID})
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

    displayed_line = str(cleandata[1][:3]).strip('/')
    trip = str(cleandata[1][3:].strip('/'))
    if trip == "":
        trip = int(0)
    else:
        trip = int(trip)

    return {
        "vehicle_id": int(cleandata[0]),
        "on_trip": ontrip,
        "line_displayed": displayed_line,
        "trip": trip,
        "end_stop": cleandata[2],
        "current_stop": cleandata[3],
        "delay_according_to_OIS": int(cleandata[4]),
        "agency": cleandata[5],
        "accessible": accessible,
        "last_ping": cleandata[8]
    }


def fetch_bus_info():
    pass

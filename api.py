from flask import Flask
from flask import request
from services.core.directsservice import DirectService
from services.core.flightservice import FlightService
from services.core.recoservice_df import RecommendationService
import json
import datetime
import pandas as pd
from model.flight import Flight

app = Flask(__name__)
print(__name__)

@app.route('/')
def get_main():
    return "hello"


@app.route('/recos',methods=['POST'])
def get_recommendations():
    input = json.loads(request.data)
    flyFrom = input["flyFrom"]
    flyTo = input["flyTo"]
    dateFrom = input["dateFrom"]
    dateTo = input["dateTo"]
    exclusions = input["exclusions"]

    #RecommendationService().get_recommendations(get_flights_temp(), ["EWR", "JFK"], "LHR", [], 0, 100)

    #recos = RecommendationService().get_recommendations(get_flights_temp(),["EWR","JFK"],"LHR",[],0,100 )

    #return json.dumps([ob.__dict__ for ob in recos])

    return "first"

def get_recommendations(flights, fly_from, fly_to, date_from, date_to, exclusions):
    print("hello")
    flights = FlightService().get_flights(date_from, date_to)
    RecommendationService().get_recommendations(flights, fly_from, fly_to, exclusions, 0, 100)

@app.route('/addFlights',methods=['POST'])
def add_flights():
    input = json.loads(request.data)
    flyFrom = input["flyFrom"]
    flyTo = input["flyTo"]
    dateFrom = input["dateFrom"]
    dateTo = input["dateTo"]

    FlightService().get_flights( flyFrom,flyTo,dateFrom, dateTo )


@app.route('/directs', methods = ['POST'])
def get_directs():
    input = json.loads(request.data)
    flyFrom = input["flyFrom"]

    directs = DirectService().get_directs(flyFrom)

    return json.dumps([ob.__dict__ for ob in directs])

'''
def get_flights_temp():
    f1 = Flight("JFK", "LHR", 1000, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),
                datetime.datetime(2019, 5, 10, 6, 0, 0), "123")
    f2 = Flight("JFK", "MIA", 50, "DU", "", datetime.datetime(2019, 5, 5, 11, 0, 0),
                datetime.datetime(2019, 5, 5, 6, 0, 0), "456")
    f3 = Flight("MIA", "LHR", 100, "DU", "", datetime.datetime(2019, 5, 8, 11, 0, 0),
                datetime.datetime(2019, 5, 8, 6, 0, 0), "456")
    f4 = Flight("LHR", "EWR", 450, "DU", "", datetime.datetime(2019, 5, 18, 11, 0, 0),
                datetime.datetime(2019, 5, 18, 6, 0, 0), "123")
    f5 = Flight("LHR", "ATL", 200, "DU", "", datetime.datetime(2019, 5, 18, 11, 0, 0),
                datetime.datetime(2019, 5, 18, 6, 0, 0), "123")
    f6 = Flight("ATL", "JFK", 300, "DU", "", datetime.datetime(2019, 5, 20, 11, 0, 0),
                datetime.datetime(2019, 5, 20, 6, 0, 0), "123")
    f7 = Flight("EWR", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),
                datetime.datetime(2019, 5, 16, 6, 0, 0), "123")
    f8 = Flight("JFK", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),  # dup of f1 but higher price
                datetime.datetime(2019, 5, 16, 6, 0, 0), "123")
    f9 = Flight("JFK", "LHR", 5, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),  # dup of f1 but on same day
                datetime.datetime(2019, 5, 10, 6, 0, 0), "123")
    fs = [f1, f2, f3, f4, f5, f6, f7, f8, f9]
    flights = pd.DataFrame.from_records([f.to_dict() for f in fs])
    return flights
'''

app.run(port=5002)


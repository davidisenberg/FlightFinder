print("what")

from flask import Flask
from flask_cors import CORS
from flask import request
from services.core.directsservice import DirectService
from services.core.flightservice import FlightService
from services.core.recoservice_df import RecommendationService
from flask import render_template
from model.flight import Flight
import pandas as pd
import json
import datetime

print ("hello")

if __name__ == '__main__':
    app = Flask(__name__, static_folder="static", template_folder="templates")
else:
    app = Flask(__name__)

CORS(app)
print(__name__)

@app.route('/hello')
def get_main():
    return "hello"

@app.route('/')
def get_ui():
    return render_template('index.html')

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    return x

@app.route('/recos',methods=['POST'])
def get_recommendations():
    try:
        print("yes yes")
        input = json.loads(request.data)
        flyFrom = [input["flyFrom"]]
        flyTo = [input["flyTo"]]
        dateFrom = input["dateFrom"]
        dateTo = input["dateTo"]
        exclusions = []

        return get_recommendations(flyFrom, flyTo, dateFrom, dateTo, exclusions)
    except Exception as e:
        return json.dumps(e)

@app.route('/recostemp',methods=['POST'])
def get_sample_recommendations():
    print("hello")
    print("yes yes")
    input = json.loads(request.data)
    fly_from = [input["flyFrom"]]
    fly_to = [input["flyTo"]]
    date_from = input["dateFrom"]
    date_to = input["dateTo"]
    exclusions = []

    print("fly_to" + fly_to)

    flights = get_flights_temp()

    print("here")


    paths = RecommendationService().get_recommendations(flights, fly_from, fly_to, exclusions, 2, 10)

    print("after recos")

    list_of_paths = []
    for path in paths:
        list_of_flights = []
        for flights in path:
            list_of_flights.append(flights.to_dict())
        list_of_paths.append(list_of_flights)
    list_of_paths = json.dumps(list_of_paths, default=datetime_handler)
    return list_of_paths


def get_recommendations(fly_from, fly_to, date_from, date_to, exclusions):
    print("hello")
    flights = FlightService().get_flights(date_from, date_to)
    paths = RecommendationService().get_recommendations(flights, fly_from, fly_to, exclusions, 2, 10)
    list_of_paths = []
    for path in paths:
        list_of_flights = []
        for flights in path:
            list_of_flights.append(flights.to_dict())
        list_of_paths.append(list_of_flights)
    list_of_paths = json.dumps(list_of_paths, default=datetime_handler)
    return list_of_paths


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

if __name__ == '__main__':
    app.run(port=5002)


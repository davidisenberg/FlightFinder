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
import sys
import logging

logging.basicConfig(level=logging.DEBUG)
import logging

print ("hello world")

if __name__ == '__main__':
    app = Flask(__name__, static_folder="static", template_folder="templates")
else:
    app = Flask(__name__)

CORS(app)
print(__name__)

@app.route('/hello')
def get_main():
    app.logger.info('testing info log')
    return "hello David"

@app.route('/')
def get_ui():
    return app.send_static_file('index.html')

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    return x

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError(
            "Unserializable object {} of type {}".format(obj, type(obj))
        )

@app.route('/recos',methods=['POST'])
def get_recommendations():
    try:
        print("at the reco endpoint")
        input = json.loads(request.data)
        flyFrom = [input["flyFrom"]]
        if flyFrom == ['NYC']:
            flyFrom = ["JFK", "EWR", "LGA"]
        flyTo = [input["flyTo"]]
        dateFrom = input["dateFrom"]
        dateTo = input["dateTo"]

        days_min_int1 = input["dateMinInt1"]
        days_max_int1 = input["daysMaxInt1"]
        days_min_target = input["daysMinTarget"]
        days_max_target = input["daysMaxTarget"]
        days_min_int2 = input["daysMinInt2"]
        days_max_int2 = input["daysMaxInt2"]
        exclusions = [input["exclusions"]]

        app.logger.info(input)
        app.logger.info(flyFrom)
        app.logger.info(flyTo)
        app.logger.info(dateFrom)
        app.logger.info(dateTo)

        return get_recommendations(flyFrom, flyTo, dateFrom, dateTo, exclusions, days_min_int1, days_max_int1,
                                   days_min_target, days_max_target, days_min_int2, days_max_int2)
    except Exception as e:
        return json.dumps(e)

@app.route('/recostemp',methods=['POST'])
def get_sample_recommendations():
    app.logger.info('testing info log')
    input = json.loads(request.data)
    fly_from = input["flyFrom"]
    if fly_from == 'NYC':
        fly_from = ["JFK","EWR","LGA"]


    fly_to = [input["flyTo"]]
    date_from = input["dateFrom"]
    date_to = input["dateTo"]
    exclusions = []



    app.logger.info("fly_from: " + fly_from[0])
    app.logger.info("fly_to: " + fly_to[0])

    flights = get_flights_temp()


    # paths = RecommendationService().get_recommendations(flights, fly_from, fly_to, exclusions, 2, 10)

    paths = RecommendationService().get_recommendations(flights, fly_from, fly_to, [], 2, 10)

    if (paths == None):
        return json.dumps('{ "result": { "error": "no data returned"} }')

    app.logger.info("after recos")

    list_of_paths = []
    for path in paths:
        list_of_flights = []
        for flights in path:
            list_of_flights.append(flights.to_dict())
        list_of_paths.append(list_of_flights)
    list_of_paths = json.dumps(list_of_paths, default=date_handler)
    return list_of_paths


def get_recommendations(fly_from, fly_to, date_from, date_to, exclusions):
    print("hello")
    flights = FlightService().get_flights(date_from, date_to)
    app.logger.info(flights.shape)
    paths = RecommendationService().get_recommendations(flights, fly_from, fly_to, exclusions, 2, 10)

    app.logger.info(flights.shape)
    if (paths == None):
        return json.dumps('{ "result": { "error": "no data returned"} }')

    list_of_paths = []
    for path in paths:
        list_of_flights = []
        for flights in path:
            list_of_flights.append(flights.to_dict())
        list_of_paths.append(list_of_flights)
    list_of_paths = json.dumps(list_of_paths, default=date_handler)
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
                datetime.datetime(2019, 5, 10, 6, 0, 0), "123", datetime.date.today())
    f2 = Flight("JFK", "MIA", 50, "DU", "", datetime.datetime(2019, 5, 5, 11, 0, 0),
                datetime.datetime(2019, 5, 5, 6, 0, 0), "456", datetime.date.today())
    f3 = Flight("MIA", "LHR", 100, "DU", "", datetime.datetime(2019, 5, 8, 11, 0, 0),
                datetime.datetime(2019, 5, 8, 6, 0, 0), "456", datetime.date.today())
    f4 = Flight("LHR", "EWR", 450, "DU", "", datetime.datetime(2019, 5, 18, 11, 0, 0),
                datetime.datetime(2019, 5, 18, 6, 0, 0), "123", datetime.date.today())
    f5 = Flight("LHR", "ATL", 200, "DU", "", datetime.datetime(2019, 5, 18, 11, 0, 0),
                datetime.datetime(2019, 5, 18, 6, 0, 0), "123", datetime.date.today())
    f6 = Flight("ATL", "JFK", 300, "DU", "", datetime.datetime(2019, 5, 20, 11, 0, 0),
                datetime.datetime(2019, 5, 20, 6, 0, 0), "123", datetime.date.today())
    f7 = Flight("EWR", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),
                datetime.datetime(2019, 5, 16, 6, 0, 0), "123", datetime.date.today())
    f8 = Flight("JFK", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),  # dup of f1 but higher price
                datetime.datetime(2019, 5, 16, 6, 0, 0), "123", datetime.date.today())
    f9 = Flight("JFK", "LHR", 5, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),  # dup of f1 but on same day
                datetime.datetime(2019, 5, 10, 6, 0, 0), "123", datetime.date.today())
    fs = [f1, f2, f3, f4, f5, f6, f7, f8, f9]
    flights = pd.DataFrame.from_records([f.to_dict() for f in fs])
    return flights

if __name__ == '__main__':
    app.run(port=5003, debug=True)



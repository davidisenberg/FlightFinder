import pytest
from services.core.recoservice_df import *
from model.flight import Flight
import datetime
import queue
import pandas as pd

def setup_flights():
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
    f8 = Flight("JFK", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),   #dup of f1 but higher price
                datetime.datetime(2019, 5, 16, 6, 0, 0), "123")
    f9 = Flight("JFK", "LHR", 5, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),    #dup of f1 but on same day
                datetime.datetime(2019, 5, 10, 6, 0, 0), "123")
    fs = [f1, f2, f3, f4, f5, f6, f7, f8, f9]
    return pd.DataFrame.from_records([f.to_dict() for f in fs])

def test_get_recos():
    try:
        flights = setup_flights()
        sources = ["JFK","EWR"]
        targets = ["LHR"]
        exclusions = []

        results= RecommendationService().get_recommendations(flights, sources, targets, exclusions, 2, 10, 25)
        print(len(results) ==2)
    except Exception as e:
        print(e)


def test_update_queue_with_first_stops():

    flights = setup_flights()
    sources = ["JFK"]
    targets = ["LHR"]
    r =RecommendationService()
    q : queue.Queue = r.get_initial_queue(sources)

    #current = q.get()
    #r.update_queue_with_first_stops(q,current,flights,[],targets)

    list(q)

    return 3

test_get_recos()







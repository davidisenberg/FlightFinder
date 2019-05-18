import pytest
from services.core.flightservice import *
from model.flight import Flight
import datetime
import queue



def test_get_flights():
    flights = FlightService().get_flights("JFK","LHR",datetime.date.today(), datetime.date.today() + datetime.timedelta(days=2))

    return flights



test_get_flights()







#!/usr/bin/env python

from sqlalchemy import *
import pymysql
from sqlalchemy.orm import Session, sessionmaker
from model.flight import Flight
from  sqlalchemy.ext.declarative  import  declarative_base
import pandas as pd
import numpy as np

Base  =  declarative_base()

class FlightsDB(Base):
    __tablename__ = 'flights'
    Id = Column(Integer, primary_key=True)
    FlyFrom = Column(String)
    FlyTo = Column(String)
    Price = Column(Numeric)
    Airline = Column(String)
    ArrivalTimeUTC = Column(DateTime)
    DepartTimeUTC = Column(DateTime)
    FlightNum = Column(String)

    def __init__(self, fly_from, fly_to, price, airline, duration, arrival, depart, flight_num):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to
        self.Price = price
        self.Airline = airline
        self.Duration = duration
        self.ArrivalTimeUTC = arrival
        self.DepartTimeUTC = depart
        self.FlightNum = flight_num


class FlightsRepository:

    def __init__(self):
        self.engine = create_engine("mysql+pymysql://davidisenberg:vpnzv8zy@localhost/ff?host=localhost?port=3306")

    def get_flights(self, fly_from = None):
        Session = sessionmaker(bind=self.engine)
        s = Session()
        if fly_from is None:
            flights = pd.read_sql(s.query(FlightsDB).statement, s.bind)
        else:
            flights = pd.read_sql(s.query(FlightsDB).filter(FlightsDB.FlyFrom == fly_from).statement, s.bind)
        return flights


    def insert_flights(self, flights):
        Session = sessionmaker(bind=self.engine)
        s = Session()
        directs_dbs = []
        for flight in flights:
            directs_dbs.append( FlightsDB(flight.FlyFrom,flight.FlyTo))

        s.bulk_save_objects(directs_dbs)
        s.commit()


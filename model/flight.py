from decimal import *
import datetime

class Flight:
    FlyFrom: str
    FlyTo: str
    Price: float
    Airline: str
    Duration: str
    ArrivalTimeUTC: datetime
    DepartTimeUTC: datetime
    FlightNum: str
    AsOfDate: int

    def __init__(self, fly_from, fly_to, price, airline, duration, arrivalTimeUTC, departTimeUTC, flightNum, asOfDate):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to
        self.Price = price
        self.Airline = airline
        self.Duration = duration
        self.ArrivalTimeUTC = arrivalTimeUTC
        self.DepartTimeUTC = departTimeUTC
        self.FlightNum = flightNum
        self.AsOfDate = asOfDate

    def to_dict(self):
        return {
            'FlyFrom': self.FlyFrom,
            'FlyTo': self.FlyTo,
            'Price': self.Price,
            'Airline': self.Airline,
            'Duration': self.Duration,
            'ArrivalTimeUTC': self.ArrivalTimeUTC,
            'DepartTimeUTC': self.DepartTimeUTC,
            'FlightNum': self.FlightNum,
            'AsOfDate' : self.AsOfDate
        }
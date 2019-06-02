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
    DataDate: int

    def __init__(self, fly_from, fly_to, price, airline, duration, arrivalTimeUTC, departTimeUTC, flightNum, dataDate):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to
        self.Price = price
        self.Airline = airline
        self.Duration = duration
        self.ArrivalTimeUTC = arrivalTimeUTC
        self.DepartTimeUTC = departTimeUTC
        self.FlightNum = flightNum
        self.DataDate = dataDate

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
            'DataDate' : self.DataDate
        }
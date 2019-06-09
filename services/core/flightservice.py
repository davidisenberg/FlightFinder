from services.vendor.skypicker import SkyPickerApi
from data.flightrepository import FlightsRepository
from services.core.directsservice import DirectService
import datetime
import pandas as pd
import time

class FlightService:

    def get_flights(self, date_from, date_to):
        try:
            flights = []
            flights = FlightsRepository().get_latest_flights(date_from, date_to)
            if len(flights) > 0:
                return flights

        except Exception as e:
            print(e)

        return flights

    def get_todays_flights(self,fly_from, fly_to, date_from, date_to):
        try:
            flights = FlightsRepository().get_todays_flights(fly_from, fly_to, date_from, date_to)
            if len(flights) > 0:
               return flights

            flights = SkyPickerApi().get_flights(fly_from, fly_to, date_from, date_to)
            if len(flights) == 0:
                return pd.DataFrame()

            return flights
        except Exception as e:
            print(e)
            raise

        return flights

    def get_one_flight_for_year(self, fly_from, fly_to):
        try:
            flights = SkyPickerApi().get_flights(fly_from, fly_to, datetime.date.today(),
                                                 datetime.date.today() + datetime.timedelta(220))
            return flights
        except Exception as e:
            print(e)
            raise

    def add_one_flight_for_year(self,fly_from,fly_to):
        try:
            flights = self.get_one_flight_for_year(fly_from, fly_to)
            FlightsRepository().insert_flights(flights)
        except Exception as e:
            print(e)
            raise


    def add_all_flights(self, sources, destinations, date_from, date_to):
        try:
            flights = self.get_all_flights( sources, destinations, date_from, date_to)
            FlightsRepository().insert_flights(flights)
        except Exception as e:
            print(e)
            raise

    def get_all_flights(self, sources, destinations, date_from, date_to):
        try :
            flight_list = []
            for source in sources:
                for destination in destinations:
                    source_directs = DirectService().get_directs(source)["FlyTo"].to_list()
                    destination_directs = DirectService().get_directs(destination)["FlyTo"].to_list()
                    fl = self.get_flight_list_per_source_destination(source, destination, source_directs, destination_directs)
                    flight_list = flight_list + fl
            flight_list = list(set(flight_list))
            flights= self.get_flights_from_list(flight_list, date_from, date_to)

            return flights

        except Exception as e:
            print(e)
            raise

        return flights

    def get_flights_from_list(self, flight_list, date_from, date_to):
        try:
            appended_data = None
            for flight_pair in flight_list:
                print("GetFlights: " + flight_pair[0] + "-> " + flight_pair[1])
                flights = self.get_todays_flights(flight_pair[0], flight_pair[1], date_from, date_to)
                print("Number returned: " + str(len(flights)))
                if len(flights) == 0:
                    continue
                elif appended_data is None:
                    appended_data = flights
                else:
                    appended_data = appended_data.append(flights)

            return appended_data

        except Exception as e:
            print(e)

        return appended_data

    def get_flight_list_for_one(self, fly_from):
        directs = DirectService().get_directs(fly_from)["FlyTo"].to_list()
        flight_list = self.get_flight_list_per_source(fly_from, directs)
        return flight_list

    def get_flight_list_per_source(self, source, directs):
        try:
            flight_list = []
            for direct in directs:
                flight_list.append((source, direct))

            return flight_list
        except Exception as e:
            print(e)

    def get_flight_list_per_source_destination(self, source, destination, source_directs, destination_directs):
        try:
            flight_list = []
            for direct in source_directs:
                if direct not in destination_directs:
                    continue
                flight_list.append((source,direct))
                flight_list.append((direct, source))
                flight_list.append((direct, destination))
                flight_list.append((destination, direct))

            if(source in destination_directs):
                flight_list.append((source, destination))
                flight_list.append((destination, source))
            return flight_list
        except Exception as e:
            print(e)

        return flight_list

    def add_flights(self,  ):
        completed = []
        queue = ['LHR']

        while len(queue) > 0:
            chosen = queue[0]
            queue.remove(chosen)
            if chosen in completed:
                continue

            completed.append(chosen);
            directs = self.add_all_flights(["JFK","EWR","LGA"],[chosen],datetime.date.today(),datetime.date.today() + datetime.timedelta(days=250))
            for data in directs:
                direct = data["flyTo"]
                if direct in completed or direct in queue:
                    continue
                queue.append(direct)

#list = FlightService().get_flights_from_list([("JFK","LHR"),("LHR","JFK")],datetime.date(2019,6,1), datetime.date(2019,6,30))
#list

#list = FlightService().add_all_flights(["JFK","EWR","LGA"],["CTG"],datetime.date(2019,6,1), datetime.date(2019,9,30))
#list


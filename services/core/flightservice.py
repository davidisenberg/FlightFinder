from services.vendor.skypicker import SkyPickerApi
from data.flightrepository import FlightsRepository


class FlightService:

    def get_flights(self, fly_from, fly_to, date_from, date_to):
        try:
            flights = []
            flights = FlightsRepository().get_flights(fly_from, fly_to, date_from, date_to)
            if len(flights) > 0:
               return flights

            flights = SkyPickerApi().get_flights(fly_from, fly_to, date_from, date_to)
            FlightsRepository().insert_flights(flights)
        except Exception as e:
            print(e)

        return flights

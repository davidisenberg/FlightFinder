from services.vendor.flightsfrom import FlightFromApi
from data.directrepository import DirectRepository


class DirectService:

    def get_directs(self, fly_from):

        directs = DirectRepository().get_directs(fly_from)
        if len(directs) > 0:
            return directs

        directs = FlightFromApi().get_directs(fly_from)
        DirectRepository().insert_directs(directs)

        return directs

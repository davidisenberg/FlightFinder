from services.vendor.flightsfrom import FlightFromApi
from data.directrepository import DirectsRepository


class DirectService:

    def get_directs(self, fly_from):

        directs = DirectsRepository().get_directs(fly_from)
        if len(directs) > 0:
            return directs

        directs = FlightFromApi().get_directs(fly_from)
        DirectsRepository().insert_directs(directs)

        return directs

from services.vendor.flightsfrom import FlightFromApi
from data.directrepository import DirectRepository
import pandas as pd


class DirectService:

    def get_directs(self, fly_from):

        directs = DirectRepository().get_directs(fly_from)
        if len(directs) > 0:
            return directs

        directs = FlightFromApi().get_directs(fly_from)
        DirectRepository().insert_directs(directs)

        return directs

    def get_all_directs(self) -> pd.DataFrame:
        directs = DirectRepository().get_all_directs()
        return directs

    def get_directs_list(self):
        directs = self.get_all_directs()
        direct_list: [] = []
        for row in directs.itertuples():
            row_list = [row.FlyFrom, row.FlyTo]
            direct_list.append(row_list)
        return direct_list


#for direct in DirectService().get_directs_list():
#    print("" + direct["FlyFrom"] + " | " + direct["FlyTo"])
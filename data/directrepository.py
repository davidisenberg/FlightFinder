import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import geopy.distance

class DirectRepository:

    __directs_parquet = os.path.join("storage", "directs.parquet")
    __airports_parquet = os.path.join("storage", "airports.parquet")

    def get_all_directs(self):
        try:
            table = pq.read_table(self.__directs_parquet)
            directs = table.to_pandas().drop_duplicates()
        except Exception as e:
            print(e)
            directs = pd.DataFrame()

        return directs

    def get_directs_from_list(self, fly_from_list):
        try:
            # directs = pq.ParquetDataset(self.__root + "directs.parquet",
            #                     filters=[ [('FlyFrom', '=', "JFK")],[('FlyFrom', '=', "EWR")]]
            #                             )


            directs = pq.ParquetDataset(self.__directs_parquet,
                                        filters=[('FlyFrom', '=', "JFK")]
                                        )
            directs_pd = directs.read().to_pandas().drop_duplicates()
            return directs_pd

        except Exception as e:
            print(e)
            directs = pd.DataFrame()

        return directs

    def get_directs(self, fly_from):
        try:

            table = pq.read_table(self.__directs_parquet)
            directs = table.to_pandas().drop_duplicates()
            directs = directs[directs["FlyFrom"] == fly_from]
        except Exception as e:
            print(e)
            directs = pd.DataFrame()

        return directs


    def insert_directs(self, directs):
        try:
            table = pa.Table.from_pandas(directs)
            pq.write_to_dataset(table,
                                root_path=self.__directs_parquet,
                                partition_cols=["FlyFrom"]
                                )
        except Exception as e:
            print(e)

    def get_airports(self):
        try:
            table = pq.read_table(self.__airports_parquet)
            airports = table.to_pandas().drop_duplicates()
        except Exception as e:
            print(e)
            airports = pd.DataFrame()

        return airports

    def create_airports(self):
        directs = self.get_all_directs()
        airports = directs.groupby("FlyTo").first().reset_index().rename(columns={"FlyTo": "Iata"})[
            ["Iata", "City", "Country", "DisplayName", "Latitude", "Longitude", "NumberOfRoutes"]]

        a = airports
        for index, row in a.iterrows():
            coord = (row["Latitude"], row["Longitude"])
            a["distance"] = a.apply(self.calc_geo, coordinates_1=coord, axis=1)
            close = a[a["distance"] < 25]["Iata"].to_list()
            a.at[index, "geo25"] = ','.join(close)
            close = a[a["distance"] < 50]["Iata"].to_list()
            a.at[index, "geo50"] = ','.join(close)
            close = a[a["distance"] < 75]["Iata"].to_list()
            a.at[index, "geo75"] = ','.join(close)
            print(row["Iata"] + "|" + str(a.at[index,"geo25"]) + "|" + str(a.at[index,"geo50"]) + "|" + str(a.at[index,"geo75"]))

        a.drop(columns=["distance"])

        table = pa.Table.from_pandas(a)
        pq.write_to_dataset(table, root_path=self.__airports_parquet)

    def calc_geo(self, row, coordinates_1):
        coords_2 = (row["Latitude"], row["Longitude"])
        return geopy.distance.distance(coordinates_1, coords_2).miles







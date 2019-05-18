#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

class FlightsRepository:

    #root dir
    __root = "c:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\"

    def get_flights(self, fly_from, fly_to, date_from, date_to):
        flights : pd.DataFrame
        try:
            table = pq.read_table(self.__root + "flights.parquet")
            flights = table.to_pandas()
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights


    def insert_flights(self, flights):
        try:
            table = pa.Table.from_pandas(flights)
            pq.write_to_dataset(table,
                                root_path=self.__root + 'flights.parquet',
                                partition_cols=['FlyFrom']

                                )
        except Exception as e:
            print(e)



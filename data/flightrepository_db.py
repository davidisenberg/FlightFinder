#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

class FlightsRepository:

    __root = "c:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\"

    def get_flights(self, fly_from, fly_to, date_from, date_to):
        Session = sessionmaker(bind=self.engine)
        s = Session()
        directs = []
        for row in s.query(DirectsDB).filter(DirectsDB.FlyFrom == fly_from):
            directs.append(Direct(row.FlyFrom, row.FlyTo))
        return directs

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



#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime

class FlightsRepository:

    #root dir
    __root = "c:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\"

    def get_flights_for_dates(self, date_from, date_to):
        flights: pd.DataFrame
        try:
            table = pq.read_table(self.__root + "flights.parquet")
            flights = table.to_pandas().drop_duplicates()
            flights = flights[(flights['DepartTimeUTC'] > date_from) &
                              (flights['DepartTimeUTC'] < date_to)  ]
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights

    def get_latest_flights(self, date_from, date_to):
        try:
            pq1 = pq.ParquetDataset(self.__root + "flights.parquet",
                                    filters=[('AsOfDate', '=',
                                              int(datetime.date.today().strftime('%Y%m%d'))), ])

            pq2 = pq.ParquetDataset(self.__root + "flights.parquet",
                                    filters=[('AsOfDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-1)).strftime(
                                                  '%Y%m%d'))), ])

            flights: pd.DataFrame = pd.DataFrame()
            if len(pq1.pieces) > 0:
                flights = flights.append(pq1.read().to_pandas())
            if len(pq2.pieces) > 0:
                flights = flights.app(pq2.read().to_pandas())
            flights.drop_duplicates()

            if (len(flights) == 0):
                return pd.DataFrame()

            flights = flights[(flights['DepartTimeUTC'] >= pd.Timestamp(date_from)) &
                              (flights['DepartTimeUTC'] <= pd.Timestamp(date_to)) ]
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights


    def get_todays_flights(self, fly_from, fly_to, date_from, date_to):
        try:

            pq1 = pq.ParquetDataset(self.__root + "flights.parquet",
                                        filters=[('AsOfDate', '=',
                                                  int(datetime.date.today().strftime('%Y%m%d'))), ])

            flights : pd.DataFrame = pd.DataFrame()
            if len(pq1.pieces) > 0:
                flights = flights.append(pq1.read().to_pandas())
            flights.drop_duplicates()

            if(len(flights) == 0):
                return pd.DataFrame()

            flights = flights[(flights['DepartTimeUTC'] >= pd.Timestamp(date_from)) &
                              (flights['DepartTimeUTC'] <= pd.Timestamp(date_to)) &
                              (flights['FlyFrom'] == fly_from) &
                              (flights['FlyTo'] == fly_to)]
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights


    def insert_flights(self, flights):
        try:
            table = pa.Table.from_pandas(flights,preserve_index=False)
            pq.write_to_dataset(table,
                                root_path=self.__root + 'flights.parquet',
                                partition_cols=['AsOfDate']
                              )
        except Exception as e:
            print(e)



#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import os

class FlightsRepository:

    #root dir
    __flight_parquet = os.path.join("storage","flights.parquet")
    __flight_partial_parquet = os.path.join("storage", "flights_partial.parquet")

    def get_flights_for_dates(self, date_from, date_to):
        try:
            table = pq.read_table(self.__flight_parquet)

            flights = table.to_pandas().drop_duplicates()
            flights = flights[(flights['DepartTimeUTC'] > date_from) &
                              (flights['DepartTimeUTC'] < date_to)  ]
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights

    def get_flights_for_dates(self, data_date):
        try:
            pq1 = pq.ParquetDataset(self.__flight_parquet,
                                   filters=[('DataDate', '=', data_date)])
            flights = pq1.read().to_pandas()
            return flights
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights

    def get_latest_flights(self, date_from, date_to):
        try:

            #self.__flight_parquet = "C:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\flights.parquet"
            pq1 = pq.ParquetDataset(self.__flight_parquet,
                                      filters=[('DataDate', '=',
                                                int(datetime.date.today().strftime('%Y%m%d'))) ])


            pq2 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-1)).strftime(
                                                  '%Y%m%d')))])

            pq3 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-2)).strftime(
                                                  '%Y%m%d')))])

            pq4 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-3)).strftime(
                                                  '%Y%m%d')))])

            pq5 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-4)).strftime(
                                                  '%Y%m%d')))])

            '''
            pq1 = pq.ParquetDataset(self.__root + "flights.parquet",
                                    filters=[('DataDate', '=',
                                          int(datetime.date(2019,5,19).strftime('%Y%m%d'))), ])

            pq2 = pq.ParquetDataset(self.__root + "flights.parquet",
                                    filters=[('DataDate', '=',
                                              int((datetime.date(2019,5,19) + datetime.timedelta(days=-1)).strftime(
                                                  '%Y%m%d'))), ])

             '''
            flights: pd.DataFrame = pd.DataFrame()
            if len(pq1.pieces) > 0:
                flights = flights.append(pq1.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas())
            if len(pq2.pieces) > 0:
                flights = flights.append(pq2.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas())
            if len(pq3.pieces) > 0:
                flights = flights.append(pq3.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas())
            if len(pq4.pieces) > 0:
                flights = flights.append(pq4.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas())
            if len(pq5.pieces) > 0:
                flights = flights.append(pq5.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas())

            flights = flights[(flights['DepartTimeUTC'] >= pd.Timestamp(date_from)) &
                              (flights['DepartTimeUTC'] <= pd.Timestamp(date_to)) ]

            # way too slow
            # flights.drop_duplicates()

            if (len(flights) == 0):
                return pd.DataFrame()

        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights


    def get_todays_flights(self, fly_from, fly_to, date_from, date_to):
        try:

            pq1 = pq.ParquetDataset(self.__flight_parquet,
                                        filters=[('DataDate', '=',
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


    def insert_partial_flights(self, flights):
        try:
            table = pa.Table.from_pandas(flights,preserve_index=False)
            pq.write_to_dataset(table,
                                root_path=self.__flight_partial_parquet,
                                partition_cols=['DataDate','FlyFrom','FlyTo']
                              )
        except Exception as e:
            print(e)

    def insert_flights(self, flights):
        try:
            table = pa.Table.from_pandas(flights, preserve_index=False)
            pq.write_to_dataset(table,
                                root_path=self.__flight_parquet,
                                partition_cols=['DataDate']
                                )
        except Exception as e:
            print(e)

    def get_partial_flights(self):
        try:
            pq1 = pq.ParquetDataset(self.__flight_partial_parquet)
            flights: pd.DataFrame = pd.DataFrame()
            flights = flights.append(pq1.read().to_pandas())
            flights.drop_duplicates()

            if (len(flights) == 0):
                return pd.DataFrame()

            return flights
        except Exception as e:
            print(e)
            flights = pd.DataFrame()


        except Exception as e:
            print(e)



'''
start = datetime.datetime(2019,6,1)
end = datetime.datetime(2019,6,15)
flights = FlightsRepository().get_latest_flights(start, end)
flights
'''
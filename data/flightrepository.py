#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import os
import time

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

            start = time.time()
            pq1 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today()).strftime(
                                                  '%Y%m%d')))])
            index = 1
            while len(pq1.pieces) == 0 and index < 22:
                pq1 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              int((datetime.date.today() + datetime.timedelta(days=-index)).strftime(
                                                  '%Y%m%d')))])
                index = index + 1

            flights: pd.DataFrame = pd.DataFrame()
            if len(pq1.pieces) > 0:
                flights = pq1.read(columns=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC","Price"]).to_pandas()


            flights = flights[(flights['DepartTimeUTC'] >= pd.Timestamp(date_from)) &
                              (flights['DepartTimeUTC'] <= pd.Timestamp(date_to)) ]
            end = time.time()
            print("time to get dataframe: " + str(end-start))

            # this could keep old flights if the times changed, and could remove different airlines flights that happenedd to be on the same day
            #flights.drop_duplicates(keep='last',inplace=True, subset=["FlyFrom","FlyTo","DepartTimeUTC","ArrivalTimeUTC"])

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
                                                  int(datetime.date.today().strftime('%Y%m%d'))) ])

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
                raise Exception("There are no flights in this partial parquet file. I cannot allow this.")

            return flights
        except Exception as e:
            print(e)
            raise




'''
start = datetime.datetime(2019,6,1)
end = datetime.datetime(2019,6,15)
flights = FlightsRepository().get_latest_flights(start, end)
flights
'''
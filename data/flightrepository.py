#!/usr/bin/env python


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import datetime
import os
import time
import gc
import traceback
import shutil

class FlightsRepository:

    #root dir
    __flight_archive_dir = os.path.join("storage", str(int((datetime.date.today()).strftime('%Y%m%d'))))
    __flight_archive_parquet = os.path.join("storage",str(int((datetime.date.today()).strftime('%Y%m%d'))), "flights_archive.parquet")
    __flight_temp_parquet = os.path.join("storage", "flights_temp.parquet")
    __flight_parquet = os.path.join("storage", "flights.parquet")
    __flight_partial_parquet = os.path.join("storage", "flights_partial.parquet")
    __flight_dataset_parquet = os.path.join("storage", "flights_dataset.parquet")

    def get_flights_for_dates(self, date_from, date_to):
        try:
            f1 = self.get_flights()
            f1 = f1[f1["DepartTimeUTC"] > date_from]
            f1 = f1[f1["DepartTimeUTC"] < date_to]

            return f1

        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights

    def count_flights(self, data_date):
        try:
            pq1 = pq.read_table(self.__flight_parquet)
            return pq1.num_rows

        except Exception as e:
            print(e)
            raise

    def get_flights(self):
        try:
            print("getting data")
            start = time.time()
            flights = pq.read_table(self.__flight_parquet).to_pandas() 
            endreading = time.time()
            #flights = table.to_pandas()
            print("readtime: " + str(endreading - start))

            self.downcast_numerics(flights)
            enddowncasting = time.time()
            print("downcasting time: " + str(enddowncasting - endreading))

            return flights
        except Exception as e:
            print(e)
            flights = pd.DataFrame()

        return flights


    def get_flights_parquet_dataset(self):
        pq1 = pq.ParquetDataset(self.__flight_dataset_parquet,
                                filters=[('DataDate', '=',"20190705")])
        flights = pq1.read().to_pandas()

        return flights


    #deprecated as we're no longer using parquet dataset
    def get_latest_flights(self, date_from, date_to):
        try:

            start = time.time()
            print("checking directory: " + self.__flight_parquet )

            files = folders = 0

            for _, dirnames, filenames in os.walk(self.__flight_parquet):
                # ^ this idiom means "we won't be using this value"
                files += len(filenames)
                folders += len(dirnames)

            print("{:,} files, {:,} folders".format(files, folders))

            print("checking file: 'DataDate', '='," + str(int((datetime.date.today()).strftime('%Y%m%d'))))
            pq1 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              str(int((datetime.date.today()).strftime(
                                                  '%Y%m%d'))))])
            index = 1
            while len(pq1.pieces) == 0 and index < 22:
                print("checking file: 'DataDate', '='," + str(int((datetime.date.today() + datetime.timedelta(days=-index)).strftime(
                                                  '%Y%m%d'))))
                pq1 = pq.ParquetDataset(self.__flight_parquet,
                                    filters=[('DataDate', '=',
                                              str(int((datetime.date.today() + datetime.timedelta(days=-index)).strftime(
                                                  '%Y%m%d'))))])
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
            print(traceback.format_exc())
            flights = pd.DataFrame()

        return flights

    # deprecated as we're no longer using parquet dataset
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
            pq.write_table(table,self.__flight_parquet)
        except Exception as e:
            print(e)

    def get_partial_flights(self):
        try:
            pq1 = pq.ParquetDataset(self.__flight_partial_parquet)
            flights: pd.DataFrame = pd.DataFrame()
            flights = pq1.read().to_pandas()

            if (len(flights) == 0):
                raise Exception("There are no flights in this partial parquet file. I cannot allow this.")

            return flights
        except Exception as e:
            print(e)
            raise

    def archive_current_flights(self):
        if os.path.exists(self.__flight_parquet):
            if ~os.path.exists(self.__flight_archive_dir):
                os.mkdir(self.__flight_archive_dir)
            shutil.move(self.__flight_parquet, self.__flight_archive_parquet)


    def make_temp_current(self):
        if os.path.exists(self.__flight_temp_parquet):
            shutil.move(self.__flight_temp_parquet, self.__flight_parquet)


    def insert_flights_temp_pyarrow(self, flight_table):
        try:
            gc.collect()
            print("writing")
            pq.write_table(flight_table,self.__flight_temp_parquet)

        except Exception as e:
            print(e)

    def get_partial_flights_pyarrow(self):
        try:

            print("getting")
            pq1 = pq.ParquetDataset(self.__flight_partial_parquet)

            print("reading")
            flights = pq1.read()

            if (len(flights) == 0):
                raise Exception("There are no flights in this partial parquet file. I cannot allow this.")

            return flights
        except Exception as e:
            print(e)
            raise

    def categorize_object_columns(self, df: pd.DataFrame):

        cols = df.select_dtypes(include=['object'])

        smaller_df = pd.DataFrame()
        for col in list(df.columns):
            if(df[col].dtype.name == "object"):
                smaller_df[col] = df[col].astype('category')
            else:
                smaller_df[col] = df[col]

        return smaller_df

    def downcast_numerics(self, df: pd.DataFrame):

        floats = df.select_dtypes(include=['floating']).columns
        ints = df.select_dtypes(include=['integer']).columns
        columns_to_convert = dict()

        columns_to_convert.update({col: "float" for col in floats})
        columns_to_convert.update({col: "integer" for col in ints})

        for col, dtype in columns_to_convert.items():
            df[col] = pd.to_numeric(df[col],downcast=dtype)




#if __name__ == "__main__":
#    self.__flight_parquet = "C:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\flights.parquet"
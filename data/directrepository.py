import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os

class DirectRepository:

    __directs_parquet = os.path.join("storage", "directs.parquet")

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





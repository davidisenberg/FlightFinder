import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

class DirectRepository:

    __root = "c:\\Users\\Dave\\PycharmProjects\\FlightFinder\\storage\\"

    def get_directs(self, fly_from):
        directs: pd.DataFrame
        try:
            table = pq.read_table(self.__root + "directs3.parquet")
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
                                root_path=self.__root + 'directs3.parquet',
                                partition_cols=["FlyFrom"]
                                )
        except Exception as e:
            print(e)





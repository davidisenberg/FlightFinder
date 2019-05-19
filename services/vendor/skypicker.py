import requests
import js2py
from bs4 import BeautifulSoup
from model.direct import Direct
import pandas as pd
import datetime
import json
import pickle
from model.flight import Flight
import pandas.io.json as pdio

class SkyPickerApi:

    def get_flights(self,fly_from, fly_to, date_from, date_to):
        url = self.get_request(fly_from, fly_to, date_from, date_to)
        return self.call_api(url)


    def get_request(self,fly_from, fly_to, date_from : datetime, date_to: datetime):
        date_from = date_from.strftime("%d/%m/%Y")
        date_to = date_to.strftime("%d/%m/%Y")
        return "https://api.skypicker.com/flights?fly_from=" + fly_from + "&fly_to=" + fly_to + "&date_from=" + date_from + "&date_to=" + date_to + "&curr=USD&direct_flights=1&limit=10000&dtime_from=07:25";


    def call_api(self,url):
        try:
            #fo = open("testdata.obj", 'rb')
            #data = pickle.load(fo)
            data = requests.get(url).text

            #fileObject = open("testdata.obj", 'wb')
            #pickle.dump(data, fileObject)
            #fileObject.close()

            sp_flights = json.loads(data)["data"]

            flights = []
            for sp_flight in sp_flights:
                flights.append(Flight(sp_flight["flyFrom"], sp_flight["flyTo"], sp_flight["price"],
                                      sp_flight["airlines"][0], sp_flight["fly_duration"],
                                      sp_flight["aTimeUTC"], sp_flight["dTimeUTC"] ,sp_flight["route"][0]["flight_no"],
                                      datetime.date.today()))

            #df1 = pdio.json_normalize(data2)
            #df2 = pd.DataFrame.from_records(data2, columns=["flyTo","flyFrom","price","airlines"])

            df = pd.DataFrame.from_records([f.to_dict() for f in flights])
            if len(df) == 0:
                return pd.DataFrame()

            df['ArrivalTimeUTC'] = pd.to_datetime(df['ArrivalTimeUTC'], unit='s')
            df['DepartTimeUTC'] = pd.to_datetime(df['DepartTimeUTC'], unit='s')
            return df
        except Exception as e:
            print(e)

        return pd.DataFrame()


#SkyPickerApi().get_flights("JFK","LHR",datetime.date.today(), datetime.date.today() + datetime.timedelta(days=2))







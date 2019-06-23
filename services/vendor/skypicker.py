import requests
import pandas as pd
import datetime
import json
import pickle
#from model.flight import Flight
#import pandas.io.json as pdio

class SkyPickerApi:

    def get_flights(self,fly_from, fly_to, date_from, date_to):
        url = self.get_request(fly_from, fly_to, date_from, date_to)
        return self.call_api(url)


    def get_request(self,fly_from, fly_to, date_from : datetime, date_to: datetime):
        date_from = date_from.strftime("%d/%m/%Y")
        date_to = date_to.strftime("%d/%m/%Y")
        return "https://api.skypicker.com/flights?fly_from=" + fly_from + "&fly_to=" + \
               fly_to + "&date_from=" + date_from + "&date_to=" + \
               date_to + "&curr=USD&direct_flights=1&limit=10000&dtime_from=07:25";


    def call_api(self,url):
        try:

            fo = open("testdata.obj", 'rb')
            data = pickle.load(fo)
            #data = requests.get(url).text

            # fileObject = open("testdata.obj", 'wb')
            # pickle.dump(data, fileObject)
            # fileObject.close()

            #data = requests.get(url).text

            #sp_flights = json.loads(data)["data"]

            # flights = []
            # for sp_flight in sp_flights:
            #     flights.append({'FlyFrom': sp_flight["flyFrom"],
            #                     'FlyTo': sp_flight["flyTo"],
            #                     'Price': sp_flight["price"],
            #                     'Airline': sp_flight["airlines"][0],
            #                     'Duration': sp_flight["fly_duration"],
            #                     'ArrivalTimeUTC': sp_flight["aTimeUTC"],
            #                     'DepartTimeUTC': sp_flight["dTimeUTC"],
            #                     'FlightNum': sp_flight["route"][0]["flight_no"],
            #                     'DataDate': int(datetime.date.today().strftime('%Y%m%d'))})
            #
            # df = pd.DataFrame(flights)
            # if len(df) == 0:
            #     return pd.DataFrame()
            df = pd.DataFrame()
            df["DataDate"] = 1
            df["FlyFrom"] = 2
            df["FlyTo"] = 3

        except Exception as e:
            print(e)
            raise

        return pd.DataFrame()

    def call_api_orig(self, url):
        try:
            # fo = open("testdata.obj", 'rb')
            # data = pickle.load(fo)
            data = requests.get(url).text

            # fileObject = open("testdata.obj", 'wb')
            # pickle.dump(data, fileObject)
            # fileObject.close()

            sp_flights = json.loads(data)["data"]

            flights = []
            for sp_flight in sp_flights:
                flights.append({'FlyFrom': sp_flight["flyFrom"],
                                'FlyTo': sp_flight["flyTo"],
                                'Price': sp_flight["price"],
                                'Airline': sp_flight["airlines"][0],
                                'Duration': sp_flight["fly_duration"],
                                'ArrivalTimeUTC': sp_flight["aTimeUTC"],
                                'DepartTimeUTC': sp_flight["dTimeUTC"],
                                'FlightNum': sp_flight["route"][0]["flight_no"],
                                'DataDate': int(datetime.date.today().strftime('%Y%m%d'))})

            df = pd.DataFrame(flights)
            if len(df) == 0:
                return pd.DataFrame()

            df['ArrivalTimeUTC'] = pd.to_datetime(df['ArrivalTimeUTC'], unit='s')
            df['DepartTimeUTC'] = pd.to_datetime(df['DepartTimeUTC'], unit='s')
            return df
        except Exception as e:
            print(e)
            raise

        return pd.DataFrame()

    def get_json(self, url):
        data = requests.get(url).text

        # fileObject = open("testdata.obj", 'wb')
        # pickle.dump(data, fileObject)
        # fileObject.close()

        sp_flights = json.loads(data)["data"]
        return sp_flights

    def json_to_pd(self, sp_flights):
        flights = []
        for sp_flight in sp_flights:
            flights.append({'FlyFrom': sp_flight["flyFrom"],
                            'FlyTo': sp_flight["flyTo"],
                            'Price': sp_flight["price"],
                            'Airline': sp_flight["airlines"][0],
                            'Duration': sp_flight["fly_duration"],
                            'ArrivalTimeUTC': sp_flight["aTimeUTC"],
                            'DepartTimeUTC': sp_flight["dTimeUTC"],
                            'FlightNum': sp_flight["route"][0]["flight_no"],
                            'DataDate': int(datetime.date.today().strftime('%Y%m%d'))})

        df = pd.DataFrame(flights)
        if len(df) == 0:
            return pd.DataFrame()

        df['ArrivalTimeUTC'] = pd.to_datetime(df['ArrivalTimeUTC'], unit='s')
        df['DepartTimeUTC'] = pd.to_datetime(df['DepartTimeUTC'], unit='s')
        return df


url = SkyPickerApi().get_request("AAL", "FAE", datetime.date.today(), datetime.date.today() + datetime.timedelta(days=2))
json = SkyPickerApi().get_json(url)
# for i in range(1000):
#     df = SkyPickerApi().json_to_pd(json)





#SkyPickerApi().get_flights("JFK","LHR",datetime.date.today(), datetime.date.today() + datetime.timedelta(days=2))
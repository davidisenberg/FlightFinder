import queue
from enum import Enum
import datetime
import pandas as pd
import decimal
import time

class FlightState(Enum):
    at_source = 0
    at_intermediate_1 = 10
    at_target = 20
    at_intermediate_2 = 30
    at_completion = 40


class FlightItem:
    current_loc: str
    current_path: []
    state: FlightState
    price: decimal

    def __init__(self, current_loc, current_path, state, price):
        self.current_loc = current_loc
        self.current_path = current_path
        self.state = state
        self.price = price

    def get_prev_arrival_time(self):
        return self.current_path[len(self.current_path) - 1]["ArrivalTimeUTC"]

    def to_dict(self):
        return {
            'current_loc': self.current_loc,
            'current_path': self.current_path,
            'state': self.state,
            'price': self.price
        }

    def __str__(self):
        result = self.current_loc + " " + self.state.name + " "
        for path in self.current_path:
            result = result + " | " + path["FlyFrom"] + " -> " + path["FlyTo"]
        # + " on " + path["DepartTimeUTC"]
        return result


class RecommendationService:

    __cache: dict = {}

    def get_recommendations(self, flights, sources, targets, exclusions, days_min, days_max):

        #print("in reccomendations")
        start = time.time()
        q: queue.Queue = self.get_initial_queue(sources)
        df: pd.DataFrame = pd.DataFrame()
        while not q.empty():
            current: FlightItem = q.get()
            if current.state == FlightState.at_completion:
                df = df.append(current.to_dict(), ignore_index=True)
            else:
                self.process(q, current, flights, exclusions, targets, sources, days_min, days_max)
                #for elem in list(q.queue):
                #    print(str(elem))
        if(df.empty):
            return None

        #path = df.sort_values(by=['price'])
        #path.to_csv(''.join(sources) + "_" + ''.join(targets) + ".csv")

        path = df.sort_values(by=['price'])["current_path"].to_list()# paths = df["current_path"].to_list()

        end = time.time()
        print("total time: " + str(end-start))

        return path


    def process(self, q, current : FlightItem, flights, exclusions, targets, sources, days_min, days_max):
        if current.state == FlightState.at_source:
            self.update_queue_at_source(q, current, flights, exclusions, targets, sources)
        elif current.state == FlightState.at_intermediate_1:
            self.update_queue_at_first_intermediate(q, current, flights, targets, sources, days_min, days_max)
        elif current.state == FlightState.at_target:
            self.update_queue_at_target(q, current, flights, exclusions, targets, sources, days_min, days_max)
        elif current.state == FlightState.at_intermediate_2:
            self.update_queue_at_second_intermediate(q, current, flights, targets, sources, days_min, days_max)



    def get_initial_queue(self, sources):
        q = queue.Queue()
        for source in sources:
            q.put_nowait(FlightItem(source, [], FlightState.at_source,0))
        return q

    def get_flights_from_cache(self, fs, loc_from, loc_to_list):

        from_key = loc_from
        from_to_key = loc_from + "|" + ''.join(loc_to_list)
        if from_to_key in self.__cache:
            f1 = self.__cache[from_to_key]
        elif loc_from in self.__cache:
            f1 = self.__cache[loc_from]
            f1 = f1[f1["FlyTo"].isin(loc_to_list)]
            #print('adding to cache: ' + from_to_key + str(f1.shape))
            self.__cache[from_to_key] = f1
        else:
            f1 = fs[fs["FlyFrom"].isin([loc_from])]
            self.__cache[from_key] = f1
            #print('adding to cache: ' + from_key + str(f1.shape))
            f1 = f1[f1["FlyTo"].isin(loc_to_list)]
            self.__cache[from_to_key] = f1
            #print('adding to cache: ' + from_to_key + str(f1.shape))
        return f1


    def update_queue_at_source(self, q, current, flight_df, exclusions, targets, sources):
        start = time.time()
        fs = flight_df
        int_destinations = fs[fs["FlyTo"].isin(targets)]["FlyFrom"].unique()
        f1 = fs[(fs["FlyFrom"].isin([current.current_loc]))]
        f1 = f1[~f1["FlyTo"].isin(exclusions)]
        f1 = f1[(f1["FlyTo"].isin(int_destinations)) | (f1["FlyTo"].isin(targets))]

        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)
        end = time.time()
        #print("Source Total:" + str(end - start))


    def update_queue_at_first_intermediate(self, q, current: FlightItem, flight_df, targets, sources, days_min, days_max):
        start = time.time()
        prev_arrival_time = current.get_prev_arrival_time()
        day1 = prev_arrival_time + datetime.timedelta(days=days_min)
        day2 = prev_arrival_time + datetime.timedelta(days=days_max)
        fs = flight_df

        f1 = self.get_flights_from_cache(fs, current.current_loc, targets)
        f1 = f1[f1["DepartTimeUTC"] > day1]
        f1 = f1[f1["DepartTimeUTC"] < day2]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)
        end = time.time()
        print("at first intermediate:" + str(end - start))

    def update_queue_at_target(self, q, current: FlightItem, flight_df, exclusions, targets, sources,
                               days_min, days_max):
        start = time.time()
        fs = flight_df
        prev_arrival_time = current.get_prev_arrival_time()
        day1 = prev_arrival_time + datetime.timedelta(days=days_min)
        day2 = prev_arrival_time + datetime.timedelta(days=days_max)
        int_destinations = fs[fs["FlyFrom"].isin(targets)]["FlyTo"].unique()

        if current.current_loc in self.__cache:
            f1 = self.__cache[current.current_loc]
            #print("target cache hit" + str(f1.shape) + current.current_loc)

        else:
            f1 = fs[fs["FlyFrom"].isin([current.current_loc])]
            self.__cache[current.current_loc] = f1

        #f1 = fs[fs["FlyFrom"].isin([current.current_loc])]
        f1 = f1[(f1["FlyTo"].isin(int_destinations)) | (f1["FlyTo"].isin(sources))]
        f1 = f1[~f1["FlyTo"].isin(exclusions)]
        f1 = f1[f1["DepartTimeUTC"] > day1]
        f1 = f1[f1["DepartTimeUTC"] < day2]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)
        end = time.time()
        print("at target:" + str(end - start))

    def update_queue_at_second_intermediate(self, q, current: FlightItem, flight_df, targets, sources, days_min, days_max):

        start = time.time()
        prev_arrival_time = current.get_prev_arrival_time()

        if current.current_loc.join(sources) + str(prev_arrival_time) in self.__cache:
            #print("big hit")
            f1 = self.__cache[current.current_loc.join(sources) + str(prev_arrival_time)]
        else:
            fs = flight_df
            day1 = prev_arrival_time + datetime.timedelta(days=days_min)
            day2 = prev_arrival_time + datetime.timedelta(days=days_max)

            # if current.current_loc.join(sources) in self.__cache:
            #     print("from to hit")
            #     f1 = self.__cache[current.current_loc.join(sources)]
            # elif current.current_loc in self.__cache:
            #     f1 = self.__cache[current.current_loc]
            #     f1 = f1[f1["FlyTo"].isin(sources)]
            #     self.__cache[current.current_loc.join(sources)] = f1
            # else:
            #     f1 = fs[fs["FlyFrom"].isin([current.current_loc])]
            #     self.__cache[current.current_loc] = f1
            #     f1 = f1[f1["FlyTo"].isin(sources)]
            #     self.__cache[current.current_loc.join(sources)] = f1
            f1 = self.get_flights_from_cache(fs, current.current_loc,sources)

            f1 = f1[f1["DepartTimeUTC"] > day1]
            f1 = f1[f1["DepartTimeUTC"] < day2]
            self.__cache[current.current_loc.join(sources) + str(prev_arrival_time)] = f1

        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)
        end = time.time()
        print("second intermediate: " + current.current_loc + " " + str(end - start))

    def get_top_results(self, df):
        return df

    @staticmethod
    def add_to_queue(q : queue.Queue, current: FlightItem, f1 : pd.DataFrame, targets: [], sources: []):
        for index, stop in f1.iterrows():
            flight_state = FlightState.at_intermediate_2
            if stop["FlyTo"] in sources:
                flight_state = FlightState.at_completion
            elif stop["FlyTo"] in targets:
                flight_state = FlightState.at_target
            elif current.state == FlightState.at_source:
                flight_state = FlightState.at_intermediate_1
            elif current.state == FlightState.at_target:
                flight_state = FlightState.at_intermediate_2

            new_path = current.current_path.copy()
            new_path.append(stop)

            new_price = current.price + stop["Price"]

            q.put(FlightItem(stop["FlyTo"], new_path, flight_state, new_price))



if __name__ == "__main__":
    start = time.time()
    from services.core.flightservice import FlightService
    flights = FlightService().get_flights(datetime.date(2019,8,1), datetime.date(2019,9,15))
    paths = RecommendationService().get_recommendations(flights, ["JFK", "EWR","LGA"], ["CTG"], [], 2, 10)
    end = time.time()
    print("really total time: " + str(end - start))
    print("count: " + str(len(paths)))
    paths

    # test one
    # from model.flight import Flight
    #
    # f1 = Flight("JFK", "LHR", 1000, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),
    #             datetime.datetime(2019, 5, 10, 6, 0, 0), "123", datetime.date.today())
    # f2 = Flight("JFK", "MIA", 50, "DU", "", datetime.datetime(2019, 5, 5, 11, 0, 0),
    #             datetime.datetime(2019, 5, 5, 6, 0, 0), "456", datetime.date.today())
    # f3 = Flight("MIA", "LHR", 100, "DU", "", datetime.datetime(2019, 5, 8, 11, 0, 0),
    #             datetime.datetime(2019, 5, 8, 6, 0, 0), "456", datetime.date.today())
    # f4 = Flight("LHR", "EWR", 450, "DU", "", datetime.datetime(2019, 5, 18, 11, 0, 0),
    #             datetime.datetime(2019, 5, 18, 6, 0, 0), "123", datetime.date.today())
    # f5 = Flight("LHR", "ATL", 200, "DU", "", datetime.datetime(2019, 5, 14, 11, 0, 0),
    #             datetime.datetime(2019, 5, 18, 6, 0, 0), "123", datetime.date.today())
    # f6 = Flight("ATL", "JFK", 300, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),
    #             datetime.datetime(2019, 5, 20, 6, 0, 0), "123", datetime.date.today())
    # f7 = Flight("EWR", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),
    #             datetime.datetime(2019, 5, 16, 6, 0, 0), "123", datetime.date.today())
    # f8 = Flight("JFK", "LHR", 1200, "DU", "", datetime.datetime(2019, 5, 16, 11, 0, 0),  # dup of f1 but higher price
    #             datetime.datetime(2019, 5, 16, 6, 0, 0), "123", datetime.date.today())
    # f9 = Flight("JFK", "LHR", 5, "DU", "", datetime.datetime(2019, 5, 10, 11, 0, 0),  # dup of f1 but on same day
    #             datetime.datetime(2019, 5, 10, 6, 0, 0), "123", datetime.date.today())
    # ffs = [f1, f2, f3, f4, f5, f6, f7, f8, f9]
    # flights = pd.DataFrame.from_records([f.to_dict() for f in ffs])
    # df = pd.DataFrame()
    #
    # sources = ["JFK", "EWR"]
    # targets = ["LHR"]
    # exclusions = []
    #
    # df = pd.DataFrame()
    # paths = RecommendationService().get_recommendations(flights, ["JFK", "EWR"], ["LHR"], [], 2, 10)
    # paths

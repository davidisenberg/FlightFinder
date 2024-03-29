import queue
from enum import Enum
import datetime
import pandas as pd
import decimal
import time
from services.core.directsservice import DirectService
import pytz

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

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        result = self.current_loc + " " + self.state.name + " "
        for path in self.current_path:
            result = result + " | " + path["FlyFrom"] + " -> " + path["FlyTo"]
        # + " on " + path["DepartTimeUTC"]
        return result


class RecommendationService:

    __cache: dict = {}
    __days_min_int1: int
    __days_max_int1: int

    __days_min_int2: int
    __days_max_int2: int

    __days_min_target: int
    __days_max_target: int

    __airport_distance: int

    __sources = []
    __targets = []

    __directs : pd.DataFrame
    __airports = pd.DataFrame

    __seen : dict = {}



    def get_recommendations(self, flights, directs, airports, sources, targets, exclusions, days_min_int1, days_max_int1,
                            days_min_target,days_max_target, days_min_int2, days_max_int2, airport_distance):

        #print("in reccomendations")
        self.__days_min_int1 = days_min_int1
        self.__days_max_int1 = days_max_int1
        self.__days_min_int2 = days_min_int2
        self.__days_max_int2 = days_max_int2
        self.__days_min_target = days_min_target
        self.__days_max_target = days_max_target
        self.__airport_distance = airport_distance
        self.__directs = directs
        self.__airports = airports

        self.__seen : dict = {}
        self.__cache: dict = {}
        start = time.time()
        q: queue.Queue = self.get_initial_queue(sources)
        df: pd.DataFrame = pd.DataFrame()
        while not q.empty():
            current: FlightItem = q.get()
            if current.state == FlightState.at_completion:
                df = df.append(current.to_dict(), ignore_index=True)
            else:
                self.process(q, current, flights, exclusions, targets, sources)
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


    def process(self, q, current : FlightItem, flights, exclusions, targets, sources):
        if current.state == FlightState.at_source:
            self.update_queue_at_source(q, current, flights, exclusions, targets, sources)
        elif current.state == FlightState.at_intermediate_1:
            self.update_queue_at_first_intermediate(q, current, flights, targets, sources)
        elif current.state == FlightState.at_target:
            self.update_queue_at_target(q, current, flights, exclusions, targets, sources)
        elif current.state == FlightState.at_intermediate_2:
            self.update_queue_at_second_intermediate(q, current, flights, targets, sources)

    def get_initial_queue(self, sources):
        q = queue.Queue()
        for source in sources:
            q.put_nowait(FlightItem(source, [], FlightState.at_source,0))
        return q

    def get_flights_from_cache(self, fs, loc_from, loc_to_list):

        from_key = "".join(loc_from)
        #from_to_key = loc_from + "|" + ''.join(loc_to_list)
        from_to_key = "".join(loc_from) + "|" + "".join(loc_to_list)
        if from_to_key in self.__cache:
            f1 = self.__cache[from_to_key]
        elif from_key in self.__cache:
            f1 = self.__cache[from_key]
            f1 = f1[f1["FlyTo"].isin(loc_to_list)]
            #print('adding to cache: ' + from_to_key + str(f1.shape))
            self.__cache[from_to_key] = f1
        else:
            f1 = fs[fs["FlyFrom"].isin(loc_from)]
            self.__cache[from_key] = f1
            #print('adding to cache: ' + from_key + str(f1.shape))
            f1 = f1[f1["FlyTo"].isin(loc_to_list)]
            self.__cache[from_to_key] = f1
            #print('adding to cache: ' + from_to_key + str(f1.shape))
        return f1

    def get_surrounding_airports(self, current):
        airports = self.__airports
        if self.__airport_distance == 25:
            return airports[airports["Iata"] == current]["geo25"].iloc[0].split(",")
        elif self.__airport_distance == 50:
            return airports[airports["Iata"] == current]["geo50"].iloc[0].split(",")
        elif self.__airport_distance == 75:
            return airports[airports["Iata"] == current]["geo75"].iloc[0].split(",")

    def get_intermediate_destinations(self, fs, targets):
        for target in targets:
            target_surrounding = self.get_surrounding_airports(target)
            int_destinations = fs[fs["FlyTo"].isin(target_surrounding)]["FlyFrom"].unique()
            final_list = set()
            for destination in int_destinations:
                temp_list = self.get_surrounding_airports(destination)
                for item in temp_list:
                    final_list.add(item)
        return list(final_list)

    def update_queue_at_source(self, q, current, flight_df, exclusions, targets, sources):
        start = time.time()
        fs = flight_df
        int_destinations = self.get_intermediate_destinations(fs, targets)
        surrounding_airports = self.get_surrounding_airports(current.current_loc)
        f1 = fs[(fs["FlyFrom"].isin(surrounding_airports))]
        f1 = f1[~f1["FlyTo"].isin(exclusions)]
        f1 = f1[(f1["FlyTo"].isin(int_destinations)) | (f1["FlyTo"].isin(targets))]

        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)
        end = time.time()
        #print("Source Total:" + str(end - start))

    # def get_simmilar_airports(self, current_airport):


    def update_queue_at_first_intermediate(self, q, current: FlightItem, flight_df, targets, sources):
        start = time.time()
        prev_arrival_time = current.get_prev_arrival_time()
        day1 = prev_arrival_time + datetime.timedelta(days=int(self.__days_min_int1))
        day2 = prev_arrival_time + datetime.timedelta(days=int(self.__days_max_int1))
        fs = flight_df

        surrounding_locations = self.get_surrounding_airports(current.current_loc)
        for target in targets:
            surrounding_targets = self.get_surrounding_airports(target)
            f1 = self.get_flights_from_cache(fs, surrounding_locations, surrounding_targets)
            f1 = f1[f1["DepartTimeUTC"] > day1]
            f1 = f1[f1["DepartTimeUTC"] < day2]
            f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
            self.add_to_queue(q, current, f1, targets, sources)

        end = time.time()
        print("at first intermediate: " +current.current_loc + " " +  str(end - start))

    def update_queue_at_target(self, q, current: FlightItem, flight_df, exclusions, targets, sources):
        start = time.time()
        fs = flight_df
        prev_arrival_time = current.get_prev_arrival_time()
        day1 = prev_arrival_time + datetime.timedelta(days=int(self.__days_min_target))
        day2 = prev_arrival_time + datetime.timedelta(days=int(self.__days_max_target))
        int_destinations = fs[fs["FlyFrom"].isin(self.get_surrounding_airports(targets[0]))]["FlyTo"].unique()

        if current.current_loc in self.__cache:
            f1 = self.__cache[current.current_loc]
        else:
            f1 = fs[fs["FlyFrom"].isin(self.get_surrounding_airports(current.current_loc))]
            self.__cache[current.current_loc] = f1

        for source in sources:
            source_airports = self.get_surrounding_airports(source)
            f1 = f1[(f1["FlyTo"].isin(int_destinations)) | (f1["FlyTo"].isin(source_airports))]
            f1 = f1[~f1["FlyTo"].isin(exclusions)]
            f1 = f1[f1["DepartTimeUTC"] > day1]
            f1 = f1[f1["DepartTimeUTC"] < day2]
            f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
            self.add_to_queue(q, current, f1, targets, sources)
            print("at target: " + current.current_loc )

        end = time.time()
        print("at target:" + str(end - start))

    def get_surrounding_from_list(self, items):
        for item in items:
            surrounding_items = self.get_surrounding_airports(item)
            final_list = set()
            for surrounding_item in surrounding_items:
               final_list.add(surrounding_item)
        return list(final_list)

    def update_queue_at_second_intermediate(self, q, current: FlightItem, flight_df, targets, sources):

        try:
            start = time.time()
            prev_arrival_time = current.get_prev_arrival_time()

            surrounding_locations = self.get_surrounding_airports(current.current_loc)
            surrounding_sources = self.get_surrounding_from_list(sources)
            if "".join(surrounding_locations).join(surrounding_sources) + str(prev_arrival_time) in self.__cache:
                #print("big hit")
                f1 = self.__cache["".join(surrounding_locations).join(surrounding_sources) + str(prev_arrival_time)]
            else:
                fs = flight_df
                day1 = prev_arrival_time + datetime.timedelta(days=int(self.__days_min_int2))
                day2 = prev_arrival_time + datetime.timedelta(days=int(self.__days_max_int2))
                f1 = self.get_flights_from_cache(fs, surrounding_locations,surrounding_sources)
                f1 = f1[f1["DepartTimeUTC"] > day1]
                f1 = f1[f1["DepartTimeUTC"] < day2]
                self.__cache["".join(surrounding_locations).join(surrounding_sources) + str(prev_arrival_time)] = f1

            if(f1.empty):
                return

            start1 = time.time()
            f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
            end1 = time.time()
            print("go: " + "".join(surrounding_locations) + " " + str(end1 - start1))
            self.add_to_queue(q, current, f1, targets, sources)
            end = time.time()
            print("second intermediate: " + "".join(surrounding_locations) + " " + str(end - start))
        except Exception as e:
            print("Exception: " + str(e))
            f1.to_csv("error.csv")
            f1.info(verbose=True)
            raise

    def get_top_results(self, df):
        return df

    def is_at_completion(self, stop, sources):
        for source in sources:
            if stop["FlyTo"] in self.get_surrounding_airports(source):
                return True
        return False

    def is_at_target(self, stop, targets):
        for target in targets:
            if stop["FlyTo"] in self.get_surrounding_airports(target):
                return True
        return False

    def add_to_queue(self, q : queue.Queue, current: FlightItem, f1 : pd.DataFrame, targets: [], sources: []):
        for index, stop in f1.iterrows():
            flight_state = FlightState.at_intermediate_2

            if(self.is_at_completion(stop, sources)):
                flight_state = FlightState.at_completion
            elif(self.is_at_target(stop, targets)):
                flight_state = FlightState.at_target
            elif current.state == FlightState.at_source:
                flight_state = FlightState.at_intermediate_1
            elif current.state == FlightState.at_target:
                flight_state = FlightState.at_intermediate_2

            new_path = current.current_path.copy()
            new_path.append(stop)

            new_price = current.price + stop["Price"]

            flightItem = FlightItem(stop["FlyTo"], new_path, flight_state, new_price)
            if hash(flightItem) in self.__seen:
                continue

            self.__seen[hash(flightItem)] = True
            q.put(flightItem)

if __name__ == "__main__":
    import os
    os.chdir("C:\\Users\\Dave\\PycharmProjects\\FlightFinder\\")
    start = time.time()
    from services.core.flightservice import FlightService
    flights = FlightService().get_flights(datetime.datetime(2019,8,1,0,0,0,0,pytz.UTC), datetime.datetime(2019,9,15,0,0,0,0,pytz.UTC))
    directs = DirectService().get_all_directs()
    airports = DirectService().get_airports()
    paths = RecommendationService().get_recommendations(flights, directs, airports, ["JFK"], ["CTG"], [], 2, 4,2,4,4,10,25)
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

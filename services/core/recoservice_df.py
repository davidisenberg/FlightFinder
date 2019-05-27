import queue
from enum import Enum
import datetime
import pandas as pd
import decimal

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

    def get_recommendations(self, flights, sources, targets, exclusions, days_min, days_max):
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
        path = df.sort_values(by=['price'])["current_path"].to_list()# paths = df["current_path"].to_list()
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

    def update_queue_at_source(self, q, current, flight_df, exclusions, targets, sources):
        fs = flight_df
        int_destinations = fs[fs["FlyTo"].isin(targets)]["FlyFrom"].unique()
        f1 = fs[(fs["FlyFrom"] == current.current_loc) &
                (~fs["FlyTo"].isin(exclusions)) &
                ((fs["FlyTo"].isin(int_destinations)) | (fs["FlyTo"].isin(targets)))]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)


    def update_queue_at_first_intermediate(self, q, current: FlightItem, flight_df, targets, sources, days_min, days_max):
        prev_arrival_time = current.get_prev_arrival_time()
        fs = flight_df
        f1 = fs[(fs["FlyFrom"] == current.current_loc) &
                (fs["FlyTo"].isin(targets)) &
                (fs["DepartTimeUTC"] > prev_arrival_time + datetime.timedelta(days=days_min)) &
                (fs["DepartTimeUTC"] < prev_arrival_time + datetime.timedelta(days=days_max))]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)

    def update_queue_at_target(self, q, current: FlightItem, flight_df, exclusions, targets, sources,
                               days_min, days_max):
        fs = flight_df
        prev_arrival_time = current.get_prev_arrival_time()
        int_destinations = fs[fs["FlyFrom"].isin(targets)]["FlyTo"].unique()
        f1 = fs[(fs["FlyFrom"] == current.current_loc) &
                (fs["DepartTimeUTC"] > prev_arrival_time + datetime.timedelta(days=days_min)) &
                (fs["DepartTimeUTC"] < prev_arrival_time + datetime.timedelta(days=days_max)) &
                (~fs["FlyTo"].isin(exclusions)) &
                ((fs["FlyTo"].isin(int_destinations)) | (fs["FlyTo"].isin(sources)))]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')
        self.add_to_queue(q, current, f1, targets, sources)

    def update_queue_at_second_intermediate(self, q, current: FlightItem, flight_df, targets, sources, days_min, days_max):

        prev_arrival_time = current.get_prev_arrival_time()
        fs = flight_df
        f1 = fs[(fs["FlyFrom"] == current.current_loc) & (fs["FlyTo"].isin(sources)) &
                (fs["DepartTimeUTC"] > prev_arrival_time + datetime.timedelta(days=days_min)) &
                (fs["DepartTimeUTC"] < prev_arrival_time + datetime.timedelta(days=days_max))]
        f1 = f1.assign(rn=f1.sort_values(['Price']).groupby(['FlyTo']).cumcount() + 1).query('rn < 2')

        self.add_to_queue(q, current, f1, targets, sources)

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




from services.vendor.flightsfrom import FlightFromApi
from data.directrepository import DirectsRepository
from model.flight import Flight
import queue
from enum import Enum
import itertools

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

    def __init__(self, current_loc, current_path, state):
        self.current_loc = current_loc
        self.current_path = current_path
        self.state = state


class RecommendationService:

    def get_graph(self, flights):
        dict = {}
        for flight in flights:
            if flight.FlyFrom not in dict:
                dict[flight.FlyFrom] = {}
            if flight.FlyTo not in dict[flight.FlyFrom]:
                dict[flight.FlyFrom][flight.FlyTo] = []
            dict[flight.FlyFrom][flight.FlyTo].append(flight)
        return dict


    def get_initial_queue(self,sources):
        q = queue.Queue()
        for source in sources:
            q.put_nowait(FlightItem(source, [], FlightState.at_source))
        return q

    
    def update_queue_with_first_stops(self, q, current, graph, exclusions : [], targets : [] ):
        if current.current_loc not in graph:
            return

        for destination in graph[current.current_loc].keys():
            first_stops = filter(lambda x: x not in exclusions, graph[current.current_loc][destination])
            first_stops = sorted(first_stops, key=lambda x: x.Price, reverse=False)
            first_stops = itertools.islice(first_stops, 2)

            for first_stop in first_stops:
                flight_state = FlightState.at_intermediate_1
                if first_stop.FlyTo in targets:
                    flight_state = FlightState.at_target

                new_path = current.current_path.copy()
                new_path.append(first_stop)

                q.put(FlightItem(destination, new_path, flight_state))


    def get_recommendations(self, flights: [], sources, targets, exclusions):

        graph = self.get_graph(flights)
        q : queue.Queue = self.get_initial_queue(sources)

        while not q.empty:
            current : FlightItem = q.get()

            if current.state == FlightState.at_source:
                self.update_queue_with_first_stops(q, current, graph, exclusions, targets)

            elif current.state == FlightState.at_intermediate_1:
                print("hello")
            elif current.state == FlightState.at_target:
                print("hello")
            elif current.state == FlightState.at_intermediate_2:
                print("hello")
            elif current.state == FlightState.at_completion:
                print("hello")

























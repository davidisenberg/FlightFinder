
workon myvirtualenv
export PYTHONPATH="/home/davidisenberg/FlightFinder/tasks:/home/davidisenberg/FlightFinder/services"
luigi --module flight_task LoadAllFlights --local-scheduler
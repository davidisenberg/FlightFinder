export WORKON_HOME=~/.virtualenvs
VIRTUALENVWRAPPER_PYTHON='/usr/bin/python3'
source /usr/local/bin/virtualenvwrapper.sh
workon myvirtualenv
cd /home/davidisenberg/FlightFinder
export PYTHONPATH="/home/davidisenberg/FlightFinder/tasks:/home/davidisenberg/FlightFinder/services:/home/davidisenberg/FlightFinder"
luigi --module flight_task LoadAllFlights --local-scheduler
import datetime
import time
from services.core.directsservice import DirectService
from services.core.flightservice import FlightService
import os
import luigi


#set PYTHONPATH=%PYTHONPATH%;C:\Users\Dave\PycharmProjects\FlightFinder\tasks;C:\Users\Dave\PycharmProjects\FlightFinder\services;C:\Users\Dave\PycharmProjects\FlightFinder
#luigi --module flight_task LoadAllFlights --local-scheduler



class LoadFlight(luigi.Task):
    __local_target = os.path.join("storage", "local_targets")
    fly_from = luigi.Parameter()
    fly_to = luigi.Parameter()
    date = luigi.DateParameter()

    def output(self):
        name = "" + self.fly_from + "_" + self.fly_to
        path = os.path.join(self.__local_target,"Data_ " + name)
        return luigi.LocalTarget(path)

    def run(self):
        time.sleep(.300)
        FlightService().add_one_flight_for_year(self.fly_from,self.fly_to)
        with self.output().open('w') as f:
            f.write('%s' % self.fly_from)


class LoadAllFlights(luigi.Task):
    __local_target = os.path.join("storage", "local_targets")

    def output(self):
       #return luigi.LocalTarget('Complete_' + datetime.date.today().strftime('%Y%m%d') + '.txt')
       path = os.path.join(self.__local_target, "Complete.txt")
       return luigi.LocalTarget(path)

    def run(self):
        directs = DirectService().get_directs_list()[:1]
        today = datetime.date.today()

        FLY_FROM = 0
        FLY_TO = 1
        get_flight_task = [LoadFlight(fly_from=direct[FLY_FROM],fly_to=direct[FLY_TO],date=today)
                           for direct in directs]
        yield get_flight_task

        with self.output().open('w') as f:
            f.write('Tada!')



if __name__ == '__main__':
    luigi.run()




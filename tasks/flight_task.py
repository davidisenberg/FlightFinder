import datetime
import time
from services.core.directsservice import DirectService
from services.core.flightservice import FlightService
import os
import luigi
import shutil

#set PYTHONPATH=%PYTHONPATH%;C:\Users\Dave\PycharmProjects\FlightFinder\tasks;C:\Users\Dave\PycharmProjects\FlightFinder\services;C:\Users\Dave\PycharmProjects\FlightFinder
#luigi --module flight_task LoadAllFlights --local-scheduler

class LoadFlight(luigi.Task):
    __local_target = os.path.join("storage", "local_targets")
    fly_from = luigi.Parameter()
    fly_to = luigi.Parameter()
    date = luigi.DateParameter()

    def output(self):
        name = "" + self.fly_from + "_" + self.fly_to
        path = os.path.join(self.__local_target,"Data_" + name)
        return luigi.LocalTarget(path)

    def run(self):
        if self.fly_from != 'LEH' and self.fly_to != 'LEH':
            time.sleep(.100)
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
        directs = DirectService().get_directs_list()
        today = datetime.date.today()

        FLY_FROM = 0
        FLY_TO = 1
        get_flight_task = [LoadFlight(fly_from=direct[FLY_FROM],fly_to=direct[FLY_TO],date=today)
                           for direct in directs]
        yield get_flight_task

        with self.output().open('w') as f:
            f.write('Tada!')

class CreateDailyFile(luigi.Task):
    __local_target = os.path.join("storage", "local_targets")
    __partial_files = os.path.join("storage", "flights_partial.parquet")

    def requires(self):
        return LoadAllFlights()

    def output(self):
        # return luigi.LocalTarget('Complete_' + datetime.date.today().strftime('%Y%m%d') + '.txt')
        path = os.path.join(self.__local_target, datetime.date.today().strftime('%Y%m%d') + "done.txt")
        #path = os.path.join(self.__local_target, "David.txt")
        return luigi.LocalTarget(path)

    def run(self):
        print("starting consolidating parquets")

        FlightService().consolidate_partials_pyarrow()

        print("finished consolidating parquets, counting rows now:")

        count = FlightService().count_flights_at_date(datetime.date.today().strftime('%Y%m%d'))

        print("row count: " + str(count))

        if(count > 2):
            with self.output().open('w') as f:
                f.write('Yep, done for day... and what!')


class CleanUp(luigi.Task):
    __local_target = os.path.join("storage", "local_targets")
    __partial_files = os.path.join("storage", "flights_partial.parquet")

    def requires(self):
        return CreateDailyFile()

    def output(self):
        # return luigi.LocalTarget('Complete_' + datetime.date.today().strftime('%Y%m%d') + '.txt')
        path = os.path.join(self.__local_target, datetime.date.today().strftime('%Y%m%d') + ".txt")
        # path = os.path.join(self.__local_target, "David.txt")
        return luigi.LocalTarget(path)

    def run(self):

        for the_file in os.listdir(self.__partial_files):
            file_path = os.path.join(self.__partial_files, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)

        for the_file in os.listdir(self.__local_target):
            if the_file.startswith("20"):
                continue
            file_path = os.path.join(self.__local_target, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

        with self.output().open('w') as f:
            f.write('Yep, done for day... and what!')



if __name__ == '__main__':
    luigi.run()




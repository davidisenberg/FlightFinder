import luigi
from services.core.flightservice import FlightService as fs


class PrintNumbers(luigi.Task):
    print("here")
    n = luigi.IntParameter()

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("numbers_up_to_{}.txt".format(self.n))

    def run(self):
        try:
            with self.output().open('w') as f:
                for i in range(1, self.n + 1):
                    print("{}\n".format(i))
                    f.write("{}\n".format(i))

        except Exception as e:
            print("Exception: " + e)



class SquaredNumbers(luigi.Task):
    print("there")
    n = luigi.IntParameter()

    def requires(self):
        return [PrintNumbers(n=self.n)]

    def output(self):
        return luigi.LocalTarget("squares_up_to_{}.txt".format(self.n))

    def run(self):
        print("running")
        try:
            with self.input()[0].open() as fin, self.output().open('w') as fout:
                for line in fin:
                    n = int(line.strip())
                    out = n * n
                    fout.write("{}:{}\n".format(n, out))
                    print("{}:{}\n".format(n, out))

        except Exception as e:
            print("Exception: " + e)



if __name__ == '__main__':
    luigi.run()
#python tasks.py SquaredNumbers --local-scheduler --n 20
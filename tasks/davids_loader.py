import random as rnd
import time
from services.core.directsservice import DirectService

import luigi

class Data(luigi.Task):
    fly_from = luigi.Parameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget('Data_%s.txt' % self.fly_from)

    def run(self):
        time.sleep(1)
        with self.output().open('w') as f:
            f.write('%s' % self.fly_from)


class Dynamic(luigi.Task):
    seed = luigi.IntParameter(default=1)


    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget('Dynamic_%d.txt' % self.seed)

    def run(self):
        # This could be done using regular requires method
        jfk_directs = DirectService().get_directs("JFK")["FlyTo"].to_list()[:3]

        # ... but not this
        data_dependent_deps = [Data(fly_from=x) for x in jfk_directs]
        yield data_dependent_deps

        with self.output().open('w') as f:
            f.write('Tada!')


if __name__ == '__main__':
    luigi.run()
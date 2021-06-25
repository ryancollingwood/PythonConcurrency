from time import sleep
from datetime import datetime
from pathlib import Path
from multiprocessing import current_process
import csv


class Processor:
    def __init__(self, method, number_workers, sleep_time):
        self.create_dirs()

        self.results = list()
        self.logs = list()

        self.method = method
        self.number_workers = number_workers
        self.sleep_time = sleep_time
        self.number_tasks = None

    def log(self, message):
        when = datetime.now()
        number_tasks = self.number_tasks
        number_workers = self.number_workers
        sleep_time = self.sleep_time

        print(f"{when.time()} -{self.method}- {number_tasks} - {number_workers} - {sleep_time} - {message}")
        self.logs.append((when, number_tasks, number_workers, sleep_time, message,))

    def sort_results(self):
        self.results.sort(key=lambda x: x[0])

    def output_filename(self):
        return f'output/output-{self.method}-{self.number_tasks}_workers'\
            f'-{self.number_workers}_sleep-{str(self.sleep_time).replace(".", "_")}.csv'

    def save_results(self):
        number_results = len(self.results)
        self.log(f"number of records {number_results}")
        self.log(f"number of tasks {self.number_tasks}")
        
        try:
            assert(number_results == self.number_tasks)
        except AssertionError as e:
            self.log(f"Number of returned results doesn't equal number of tasks to be created")
            raise e

        with open(self.output_filename(), mode='w') as csv_file:
            fieldnames = ['index', 'start', 'end', 'worker', 'value']

            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(fieldnames)
            writer.writerows(self.results)

        self.log("saved data")

        self.logs("writing out logs")
        
        with open(f'logs.csv', mode='a') as csv_file:
            fieldnames = ["when", "num_tasks", "workers", "sleep_time", "message"]

            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(fieldnames)
            writer.writerows(self.logs)

        self.logs.clear()


    @staticmethod
    def create_dirs():
        for path_name in ["output", "log"]:
            try:
                Path(f"{path_name}/").mkdir()
            except FileExistsError:
                pass
            except Exception as e:
                raise e
            else:
                print(f"Created directory: {path_name}")
    
    def collect_result(self, result):
        self.results.append(result)

    def do_work(self, index, value, sleep_time):
        # TODO: replace current_process().name
        # with a call to method overriden in
        # descendant classes
        start =  datetime.now()
        mod = 1.0 / value
        sleep(sleep_time * mod)
        end =  datetime.now()
        
        return (index, start, end, str(current_process().name), value)

    def process(self, tests):
        raise NotImplementedError()

    def on_work_complete(self):
        raise NotImplementedError()

    def execute(self, data):
        raise NotImplementedError()
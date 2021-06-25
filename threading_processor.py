from processor import Processor
from concurrent import futures

class ThreadingProcessor(Processor):

    def __init__(self, number_workers, sleep_time):
        super().__init__("threads", number_workers, sleep_time)

    def on_work_complete(self, fn):
        if fn.cancelled():
            print('{}: canceled'.format(fn.arg))
        elif fn.done():
            error = fn.exception()
            if error:
                print('{}: error returned: {}'.format(
                    fn.arg, error))
            else:
                result = fn.result()
                self.collect_result(result)

    def execute(self, data):        
        self.number_tasks = len(data)
        sleep_time = self.sleep_time

        with futures.ThreadPoolExecutor(max_workers = self.number_workers) as executor:
            for i, n in enumerate(data):
                f = executor.submit(self.do_work, i, n, sleep_time)
                f.add_done_callback(self.on_work_complete)
        
        self.sort_results()
        self.save_results()

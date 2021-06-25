from processor import Processor
from multiprocessing import Pool, current_process


class MpProcessor(Processor):

    def __init__(self, number_workers, sleep_time):
        super().__init__("multiprocessing-pool", number_workers, sleep_time)

    def handle_error(self, error):
        try:
            raise error
        except ValueError:
            print("Got a value error no worries")
        except Exception as e:
            print("Got another error")
            raise e

    def on_work_complete(self, result):
        self.collect_result(result)

    def execute(self, data):
        self.number_tasks = len(data)

        sleep_time = self.sleep_time

        with Pool(self.number_workers) as p:
            try:
                self.log("sending work")

                p.starmap_async(
                    self.do_work, [(i, n, sleep_time) for i, n in enumerate(data)], 
                    callback = self.on_work_complete,
                    error_callback = self.handle_error
                    )

                self.log("work has been sent")
            except KeyboardInterrupt as e:
                p.terminate()
                p.join()
                raise e
            except Exception as e:
                print(f"ERROR: {e}")
                p.terminate()
                p.join()

            self.log("closing") 
            p.close()
            self.log("joining")
            p.join()

        self.log("work has been done")

        assert(len(self.results) > 0)

        self.results = self.results[0]

        self.sort_results()
        self.save_results()

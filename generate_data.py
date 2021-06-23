from multiprocessing import Pool, current_process
from random import randint
from time import sleep
from datetime import datetime
import os
import csv

threads = os.cpu_count() - 1
min_value = 1
max_value = 10


def do_work(value, sleep_time, *args):
    start =  datetime.now()
    mod = 1.0 / value
    sleep(sleep_time * mod)
    end =  datetime.now()
    
    return (start, end, str(current_process().name), value)


def collect_result(result):
    global results
    results.append(result)


def handle_error(error):
    try:
        raise error
    except ValueError:
        print("Got a value error no worries")
    except Exception as e:
        print("Got another error")
        raise e


def log(message):
    global logs
    when = datetime.now()
    print(f"{when.time()} - num_tasks: {num_tasks} - sleep_time: {sleep_time} - {message}")
    logs.append((when, num_tasks, sleep_time, message,))


if __name__ == "__main__":
    for test_data in (
        (100, 0),(1000, 0), (10000, 0), (100000, 0), 
        (100, 0.5),(1000, 0.5), (10000, 0.5), (100000, 0.5),
        (100, 1),(1000, 1), (10000, 1), (100000, 1),
        ):

        num_tasks = test_data[0]
        sleep_time = test_data[1]

        assert(num_tasks > 0)
        assert(sleep_time >= 0)

        global results
        global logs
        global pool_args

        results = list()
        logs = list()
        pool_args = list()

        results.clear()
        assert(len(results) == 0)
        logs.clear()

        for i in range(num_tasks):
            pool_args.append(randint(1, 10))
        
        assert(len(pool_args) > 0)

        with Pool(threads) as p:
            try:
                p.starmap_async(
                    do_work, [(i, sleep_time) for i in pool_args], 
                    callback = collect_result,
                    error_callback = handle_error
                    )
                log("work has been sent")
            except KeyboardInterrupt as e:
                p.terminate()
                p.join()
                raise e
            except Exception as e:
                print(f"ERROR: {e}")
                p.terminate()
                p.join()

            log("closing") 
            p.close()
            log("join")
            p.join()
            
        log("work has been done")

        assert(isinstance(results, list))
        assert(len(results) > 0)
        final_result = results[0]
        
        log("sorting")
        final_result.sort(key=lambda x: x[0])

        log(f"number of records {len(final_result)}")
        log(f"number of tasks {num_tasks}")
        assert(len(final_result) == num_tasks)

        with open(f'output_tasks-{num_tasks}_sleep-{sleep_time}.csv', mode='w') as csv_file:
            fieldnames = ['start', 'end', 'worker', 'value']

            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(fieldnames)
            writer.writerows(final_result)

        log("saved data")

        with open(f'logs.csv', mode='a') as csv_file:
            fieldnames = ["when", "num_tasks", "sleep_time", "message"]

            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(fieldnames)
            writer.writerows(logs)

        logs.clear()
import sys
import os
from time import sleep
from datetime import datetime
from multiprocessing import Pool, current_process
from random import randint
import csv
from pathlib import Path


def do_work(value, sleep_time):
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


def log(message, num_tasks, workers, sleep_time):
    global logs
    when = datetime.now()
    print(f"{when.time()} - num_tasks: {num_tasks} - workers: {workers} - sleep_time: {sleep_time} - {message}")
    logs.append((when, num_tasks, workers, sleep_time, message,))


def generate_data_multiprocessing_pool(num_tasks, workers, sleep_time):

    assert(num_tasks > 0)
    assert(sleep_time >= 0)

    global results
    global logs
    global pool_args

    results = list()
    logs = list()
    pool_args = list()

    log("generating data", num_tasks, workers, sleep_time)

    # generate our random numbers for our tasks
    pool_args = [randint(1, 10) for x in range(num_tasks)]

    log("data generated", num_tasks, workers, sleep_time)
    
    with Pool(workers) as p:
        try:
            log("sending work", num_tasks, workers, sleep_time)

            p.starmap_async(
                do_work, [(x, sleep_time) for x in pool_args], 
                callback = collect_result,
                error_callback = handle_error
                )

            log("work has been sent", num_tasks, workers, sleep_time)
        except KeyboardInterrupt as e:
            p.terminate()
            p.join()
            raise e
        except Exception as e:
            print(f"ERROR: {e}")
            p.terminate()
            p.join()

        log("closing", num_tasks, workers, sleep_time) 
        p.close()
        log("joining", num_tasks, workers, sleep_time)
        p.join()
    
    save_results(results, num_tasks, workers, sleep_time)


def output_filename(num_tasks, workers, sleep_time):
    return f'output/output_tasks-{num_tasks}_workers-{workers}'\
        f'_sleep-{str(sleep_time).replace(".", "_")}.csv'


def save_results(results, num_tasks, workers, sleep_time):        
    log("work has been done", num_tasks, workers, sleep_time)

    assert(len(results) > 0)
    final_result = results[0]
    
    log("sorting", num_tasks, workers, sleep_time)
    final_result.sort(key=lambda x: x[0])

    log(f"number of records {len(final_result)}", num_tasks, workers, sleep_time)
    log(f"number of tasks {num_tasks}", num_tasks, workers, sleep_time)
    
    try:
        assert(len(final_result) == num_tasks)
    except AssertionError as e:
        log(f"Number of returned results doesn't equal number of tasks to be created")
        raise e

    with open(output_filename(num_tasks, workers, sleep_time), mode='w') as csv_file:
        fieldnames = ['start', 'end', 'worker', 'value']

        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(fieldnames)
        writer.writerows(final_result)

    log("saved data", num_tasks, workers, sleep_time)

    with open(f'logs.csv', mode='a') as csv_file:
        fieldnames = ["when", "num_tasks", "workers", "sleep_time", "message"]

        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(fieldnames)
        writer.writerows(logs)

    logs.clear()


def execute(args):
    workers = os.cpu_count() - 1
    min_value = 1
    max_value = 10

    for arg in args:
        try:
            split_arg = arg.split("=")

            if split_arg[0] == "workers":
                workers = int(split_arg[1])
            elif split_arg[0] == "min_value":
                min_value = float(split_arg[1])
            if split_arg[0] == "max_value":
                max_value = float(split_arg[1])
        except Exception as e:
            print(f"Error processing argument: {arg}")
            raise e
    
    print("workers", workers)
    print("min_value", min_value)
    print("max_value", max_value)

    for path_name in ["output", "log"]:
        try:
            Path(f"{path_name}/").mkdir()
        except FileExistsError:
            pass
        except Exception as e:
            raise e
        else:
            print(f"Created directory: {path_name}")

    for test_data in (
        (100, 0),(1000, 0), (10000, 0), (100000, 0), 
        (100, 0.25),(1000, 0.25), (10000, 0.25), (100000, 0.25),
        (100, 0.5),(1000, 0.5), (10000, 0.5), (100000, 0.5),
        ):

        num_tasks = test_data[0]
        sleep_time = test_data[1]

        generate_data_multiprocessing_pool(num_tasks, workers, sleep_time)


if __name__ == "__main__":
    execute(sys.argv[1:])
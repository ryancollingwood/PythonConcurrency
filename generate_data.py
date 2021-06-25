import sys
import os
from random import randint
from pathlib import Path
from threading_processor import ThreadingProcessor
from mp_processor import MpProcessor

def seed_data(num_tasks):
    # generate our random numbers for our tasks
    return [randint(1, 10) for x in range(num_tasks)]


def execute(args):
    workers = os.cpu_count() - 1
    min_value = 1
    max_value = 10
    default_tests = (
        (100, 0),(1000, 0), (10000, 0), (100000, 0), 
        (100, 0.25),(1000, 0.25), (10000, 0.25), (100000, 0.25),
        (100, 0.5),(1000, 0.5), (10000, 0.5), (100000, 0.5),
        )
    tests = None

    for arg in args:
        try:
            split_arg = arg.split("=")

            if split_arg[0] == "workers":
                workers = int(split_arg[1])
            elif split_arg[0] == "min_value":
                min_value = float(split_arg[1])
            elif split_arg[0] == "max_value":
                max_value = float(split_arg[1])
            if split_arg[0] == "tests":
                test_params = split_arg[1].split(",")
               
                try:
                    assert(len(test_params) == 2)
                except AssertionError as e:
                    print(f"Expected 2 and only two values, got: {test_params}")
                    raise e

                tests = (
                    (int(test_params[0]), float(test_params[1])),
                    )
        except Exception as e:
            print(f"Error processing argument: {arg}")
            raise e

    if tests is None:
        tests = default_tests
    
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

    for test_data in tests:
        print(test_data)
        num_tasks = test_data[0]
        sleep_time = test_data[1]

        numbers = seed_data(num_tasks)

        processor_1 = MpProcessor(workers, sleep_time)
        processor_1.execute(numbers)

        processor_2 = ThreadingProcessor(workers, sleep_time)
        processor_2.execute(numbers)


if __name__ == "__main__":
    execute(sys.argv[1:])
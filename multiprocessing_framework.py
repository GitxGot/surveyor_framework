#!/usr/bin/env python3

import logging
import logging.handlers
import multiprocessing
import os
from pathlib import Path
import sys

LOGGING_LEVEL = logging.DEBUG
LOGGING_FILE = 'surv_log.log'
OUTPUT_FILE = 'mp_data_test.txt'
NUM_OF_WORKERS = multiprocessing.cpu_count()

#Configures the log
def log_configurer():
    root = logging.getLogger()
    h = logging.FileHandler(LOGGING_FILE)
    f = logging.Formatter(
        '%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)

# This is the listener process top-level loop: wait for logging events
# (LogRecords)on the queue and handle them, done when None
def log_writer(queue,fatal_flag):
    log_configurer()
    while not fatal_flag.is_set():
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import traceback
            print('Error in log_writer:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            fatal_flag.set()

# The worker configuration is done at the start of the worker process run.
def worker_log_configurer(queue):
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    if not (root.hasHandlers()):
        root.addHandler(h)
    root.setLevel(LOGGING_LEVEL) #try this log 1st, otherwise param

# This is the listener process loop: wait for data writing events
# on the queue and handle them, done when None.
def output_writer(data_queue, log_queue,fatal_flag):
    worker_log_configurer(log_queue)
    logger = logging.getLogger()
    with open((OUTPUT_FILE), 'w') as f:
        while not fatal_flag.is_set():
            try:
                data = data_queue.get()
                if data is None:
                    break
                if data is not None:
                    f.write(str(data))
                    logger.log(logging.DEBUG,f"Reporting Data from Queue")
            except Exception as e:
                logger.log(logging.CRITICAL,f"Issue in output_writer:\n{str(e)}")
                fatal_flag.set()
        f.flush()

# Count the number lines and characters of each file.
# Add this to data_queue.
def analyze_file(filename, log_queue, data_queue):
    worker_log_configurer(log_queue)
    logger = logging.getLogger()
    try:
        logger.log(logging.DEBUG,f"Opening File: {filename}")
        with open(filename, 'r') as file:
            lines = file.readlines()
            line_count = len(lines)
            char_count = sum(len(line) for line in lines)
        message = (f"File: {Path(filename).stem} | Lines: {line_count} | Characters: {char_count}\n")
        data_queue.put(message)
    except Exception as e:
        logger.log(logging.ERROR,
                f"Error processing file: {filename} || FILE DATA NOT UPDATED\n{str(e)}")

# Createe pool of workers. Watch files_queue generatd by finder.
# When updated add task with new filepath to pool. Continue until None
def handle_file(file_queue, data_queue, log_queue, fatal_flag):
    while not fatal_flag.is_set():
        try:
            file = file_queue.get()
            if file is None:
                break
            analyze_file(file, log_queue, data_queue)
        except Exception as e:
            worker_log_configurer(log_queue)
            logger = logging.getLogger()
            logger.log(logging.CRITICAL,f'Issue in handle_file:\n{e}')
            fatal_flag.set()

# Walk file directory, add file names to shared file queue. 
# Send None when done.
def find_file(directory_path, file_queue, log_queue, fatal_flag):
    worker_log_configurer(log_queue)
    logger = logging.getLogger()
    try:
        for root, _, filenames in os.walk(directory_path):
            for filename in filenames:
                logger.log(logging.INFO,f"Found file: {filename}")
                file_path = os.path.join(root, filename)
                file_queue.put(file_path)
        for _ in range(NUM_OF_WORKERS):
            file_queue.put(None)
    except Exception as e:
        logger.log(logging.CRITICAL,f'Issue in find_file:\n{e}')
        fatal_flag.set()

# Create log, data, file queues. Create+start log, data,file listener processes.
# Construct pool and apply handle_file. 
# Send None when done to log and data queues to stop listeners.
def main():
    open(LOGGING_FILE, 'w').close() #clean logs each run
    if len(sys.argv) < 2:
        directory_path = os.path.join('test_data')
    else:
        directory_path = os.path.join(sys.argv[1]) #assume dir to scan is in the same location as this file

    fatal_flag = multiprocessing.Manager().Event()

    log_queue = multiprocessing.Manager().Queue(-1)
    data_queue = multiprocessing.Manager().Queue(-1)
    file_queue = multiprocessing.Manager().Queue(-1)
    
    log_writer_process = multiprocessing.Process(target=log_writer,
            args=(log_queue,fatal_flag))
    log_writer_process.start()
    output_writer_process = multiprocessing.Process(target=output_writer, 
            args=(data_queue,log_queue,fatal_flag))
    output_writer_process.start()
    finder = multiprocessing.Process(target=find_file, 
            args=(directory_path, file_queue, log_queue,fatal_flag))
    finder.start()
    
    with multiprocessing.Pool(NUM_OF_WORKERS) as pool:
        results = pool.apply(handle_file, 
            args=(file_queue, data_queue, log_queue, fatal_flag))

    log_queue.put_nowait(None)
    data_queue.put_nowait(None)

    # log_queue.join()
    # data_queue.join()

if __name__ == '__main__':
    main()
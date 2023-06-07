#!/usr/bin/env python3

import logging
import logging.handlers
import multiprocessing
import os
from pathlib import Path
import sys

#Configures the log
def log_configurer():
    root = logging.getLogger()
    h = logging.FileHandler('surv_log.log')
    f = logging.Formatter(
        '%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)

# This is the listener process top-level loop: wait for logging events
# (LogRecords)on the queue and handle them, done when None
def log_queue_listener_process(queue, configurer,flag):
    configurer()
    while not flag.is_set():
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import traceback
            print('Error in log_queue_listener_process:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            flag.set()

# The worker configuration is done at the start of the worker process run.
# Note that on Windows you can't rely on fork semantics, so each process
# will run the logging configuration code when it starts.
def worker_log_configurer(queue):
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    if (root.hasHandlers()):
        root.handlers.clear()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)

# This is the listener process loop: wait for data writing events
# on the queue and handle them, done when None.
def data_queue_listener_process(data_queue, log_queue, configurer,flag):
    configurer(log_queue)
    with open(('mp_data_test.txt'), 'w') as f:
        while not flag.is_set():
            try:
                data = data_queue.get()
                if data is None:
                    break
                f.write(str(data))
                f.flush()
                logger = logging.getLogger()
                logger.log(logging.DEBUG,f"Reporting Data from Queue")
            except Exception as e:
                logger = logging.getLogger()
                logger.log(logging.CRITICAL,f"Issue in data_queue_listener_process:\n{str(e)}")
                flag.set()

# Count the number lines and characters of each file.
# Add this to data_queue.
def analyze_file_process(filename, log_queue, configurer, data_queue):
    configurer(log_queue)
    try:
        logger = logging.getLogger()
        logger.log(logging.DEBUG,f"Opening File: {filename}")
        with open(filename, 'r') as file:
            lines = file.readlines()
            line_count = len(lines)
            char_count = sum(len(line) for line in lines)
        message = (f"File: {Path(filename).stem} | Lines: {line_count} | Characters: {char_count}\n")
        data_queue.put(message)
    except Exception as e:
        logger = logging.getLogger()
        logger.log(logging.ERROR,
                f"Error processing file: {filename} || FILE DATA NOT UPDATED\n{str(e)}")

# Createe pool of workers. Watch files_queue generatd by finder.
# When updated add task with new filepath to pool. Continue until None
def file_queue_listener(file_queue, data_queue, log_queue, configurer, flag):
    configurer(log_queue)
    while not flag.is_set():
        try:
            file = file_queue.get()
            if file is None:
                break
            analyze_file_process(file, log_queue, worker_log_configurer, data_queue)
        except Exception as e:
            logger = logging.getLogger()
            logger.log(logging.CRITICAL,f'Issue in file_queue_listener:\n{e}')
            flag.set()

# Walk file directory, add file names to shared file queue. 
# Send None when done.
def file_finder_process(directory_path, file_queue, log_queue, configurer, flag):
    configurer(log_queue)
    try:
        for root, _, filenames in os.walk(directory_path):
            for filename in filenames:
                logger = logging.getLogger()
                logger.log(logging.INFO,f"Found file: {filename}")
                file_path = os.path.join(root, filename)
                file_queue.put(file_path)
        file_queue.put(None)
    except Exception as e:
        logger = logging.getLogger()
        logger.log(logging.CRITICAL,f'Issue in file_finder_process:\n{e}')
        flag.set()

# Create log, data, file queues. Create+start log, data,file listener processes.
# Construct pool and apply file_queue_listener. 
# Send None when done to log and data queues to stop listeners.
def main():
     
    open('surv_log.log', 'w').close() #clean logs each run
    if len(sys.argv) < 2:
        directory_path = os.path.join('.git')
    else:
        directory_path = os.path.join(sys.argv[1]) #assume dir to scan is in the same location as this file
    try:
        if not os.path.exists(directory_path): 
            raise Exception(f"Unable to find given directory: {directory_path}")
        flag = multiprocessing.Manager().Event()
        log_queue = multiprocessing.Manager().Queue(-1)
        data_queue = multiprocessing.Manager().Queue(-1)
        file_queue = multiprocessing.Manager().Queue(-1)
        log_listener = multiprocessing.Process(target=log_queue_listener_process,
                args=(log_queue, log_configurer,flag))
        log_listener.start()
        data_listener = multiprocessing.Process(target=data_queue_listener_process, 
                args=(data_queue,log_queue, worker_log_configurer,flag))
        data_listener.start()
        finder = multiprocessing.Process(target=file_finder_process, 
                args=(directory_path, file_queue, log_queue, worker_log_configurer,flag))
        finder.start()
        with multiprocessing.Pool() as pool:
            pool.apply(file_queue_listener, 
                args=(file_queue, data_queue, log_queue, worker_log_configurer, flag))
        log_queue.put_nowait(None)
        data_queue.put_nowait(None)
        finder.join()
        log_listener.join()
        data_listener.join()

    except Exception as e:
        log_configurer()
        logger = logging.getLogger()
        logger.log(logging.CRITICAL,f"Issue in main:\n{str(e)}")

if __name__ == '__main__':
    main()
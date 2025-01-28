import os
import threading
import time
import argparse
from os.path import isfile, join, isdir
from concurrent.futures import ThreadPoolExecutor
import progressbar
import datetime

parser = argparse.ArgumentParser(description='Optional app description')
parser.add_argument('--directory', '-d', type=str,
                    help='A required integer positional argument', required=True)
parser.add_argument('--mode', '-m', type=str, nargs='?', default='sim',
                    help='Modes: exec - Removes the files. sim - Save the expected execution into a log file')
parser.add_argument('--threads', '-th', type=int, nargs='?', default=30,
                    help='Number of threads to execute the cleanup')
parser.add_argument('--time', '-t', type=int, nargs='?', default=3600,
                    help='Time in minutes of updating removal')

main_directory = ""
files = []
time_threshold = 0


def get_tmp_folders():
    directories = []
    tmp_folders = []
    directories.append(main_directory)
    print("Detecting temporal folders")
    while len(directories) > 0:
        directory = directories.pop(0)
        candidate_folders = [f for f in os.listdir(directory) if isdir(join(directory, f))]
        if 'tmp' in candidate_folders:
            tmp_folders.append(directory + '/tmp')

        for candidate in candidate_folders:
            directories.append(directory + '/' + candidate)

    return tmp_folders


def detect_files_to_remove(file_folders):
    while len(file_folders) > 0:
        folder = file_folders.pop()
        for f in os.listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and (time.time() - os.path.getctime(file_path)) > time_threshold:
                files.append(file_path)


def save_into_log_file():
    init_files_remove = len(files)
    print("Writing files to remove")
    files_counter = {"removed": AtomicCounter(), "percentage": AtomicCounter(), "files_to_remove": init_files_remove}
    progress_bar = create_progress_bar()
    with open("output.txt", "w") as txt_file:
        for file in files:
            txt_file.write(" ".join(file) + "\n")
            removed = files_counter["removed"].increment()
            percentage = removed / files_counter["files_to_remove"]
            update_progress_bar(percentage, progress_bar, files_counter)
    progress_bar.finish()


def remove_files(files_to_remove):
    files_to_remove_quiantity = len(files_to_remove)
    counters = {"removed": AtomicCounter(), "percentage": AtomicCounter(), "files_to_remove": files_to_remove_quiantity}
    bar = create_progress_bar()
    futures_remove = [exe.submit(remove_files_multithread, files_to_remove, bar, counters) for _ in range(0, threads)]
    wait_for_execution(futures_remove)
    bar.finish()


def remove_files_multithread(file_folders, progress_bar, files_counter):
    while len(file_folders) > 0:
        file = file_folders.pop()
        os.remove(file)
        removed = files_counter["removed"].increment()
        percentage = int(removed / files_counter["files_to_remove"])
        update_progress_bar(percentage, progress_bar, files_counter)


def update_progress_bar(percentage, progress_bar, files_counter):
    if percentage > files_counter["percentage"].value:
        progress_bar.update()
        files_counter["percentage"].set(percentage)


def create_progress_bar():
    bar = progressbar.ProgressBar(max_value=100)
    bar.start()

    return bar


def wait_for_execution(futures):
    for future in futures:
        future.result()


class AtomicCounter:

    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        with self._lock:
            self.value += num
            return self.value

    def set(self, num=1):
        with self._lock:
            self.value += num
            return self.value


if __name__ == "__main__":
    args = vars(parser.parse_args())
    main_directory = args['directory']
    threads = args['threads']
    folders = get_tmp_folders()
    with ThreadPoolExecutor(max_workers=threads) as exe:
        print("Detecting files to remove")

        futures = [exe.submit(detect_files_to_remove, folders) for f in range(0, threads)]
        wait_for_execution(futures)

        if args['mode'] == 'sim':
            save_into_log_file()
        else:
            a = datetime.datetime.now()
            atomic_counter = AtomicCounter()
            init_files_remove = len(files)
            remove_files(files)
            print((datetime.datetime.now() - a).total_seconds() * 1000)

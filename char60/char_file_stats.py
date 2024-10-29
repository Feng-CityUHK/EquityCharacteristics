import pyarrow.feather as feather
import multiprocessing as mp
from math import ceil
from os.path import exists

# Using pooling in this file is a bit of overkill but nicly demonstrates how to use it
 
# Directory where the characteristic files are stored
IN_DIR = './'
# Number of CPU cores to use. 1 for debugging. Usually use 4 to prove out pool processing
CPU_CORE_COUNT = 1

# A well-known list of valid char files
CHAR_FILES = [
    'abr',              # abr.py
    'baspread',         # bid_ask_spread.py
    'beta',             # beta.py
    'chars_a_60',       # accounting_60.py
    'chars_q_60',       # accounting_60.py
    'chars_a_raw',      # merge_chars_60.py
    'chars_q_raw',      # merge_chars_60.py
    'ill',              # ill.py
    'maxret',           # maxret_d.py
    'myre',             # myre.py
    'rvar_capm',        # rvar_capm.py
    'rvar_ff3',         # rvar_ff3.py
    'rvar_mean',        # rvar_mean.py
    'std_dolvol',       # std_dolvol.py
    'std_turn',         # std_turn.py
    'sue',              # sue.py
    'zerotrade'         # zerotrade.py
]


def pool_process(files_dict, key):

    for f in files_dict[key]:
        file_path = f'{IN_DIR}{f}.feather'
        if exists(file_path):
            read_arrow = feather.read_table(
                file_path)  # Result is pyarrow.Table
            rows = read_arrow.num_rows
            cols = read_arrow.num_columns
            bytes = read_arrow.nbytes
            print(f'file: {f}.feather rows: {
                  rows:,}, cols: {cols:,}, bytes: {bytes:,}')


def split_files(start, end, step, valid_char_files):

    files_dict = {}

    for i in range(start, end, step):
        # print(f'splitting files: {i} to {min(i+step-1, end-1)}')
        files_dict[f'char{i}'] = valid_char_files[i:i+step]

    return files_dict


def main(valid_char_files):

    start = 0
    end = len(valid_char_files)
    step = int(ceil(end/CPU_CORE_COUNT))

    files_dict = split_files(start, end, step, valid_char_files)

    pool = mp.Pool(CPU_CORE_COUNT)

    for f in files_dict.keys():
        pool.apply_async(pool_process, (files_dict, f))

    pool.close()
    pool.join()


if __name__ == '__main__':

    valid_char_files = []

    for f in CHAR_FILES:

        file_path = f'{IN_DIR}{f}.feather'

        if not exists(file_path):
            print(f'WARNING: File {file_path} does not exist')
        else:
            valid_char_files.append(f)

    if len(valid_char_files) == 0:
       print('WARNING: No well-known .feather files found')
       exit(1)

    main(valid_char_files)
    
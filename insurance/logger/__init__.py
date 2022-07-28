import logging
from datetime import datetime
import os
import pandas as pd
from insurance.constant import get_current_time_stamp

def get_log_file_name():
    return f"log_{get_current_time_stamp()}.log"

LOG_DIR="logs"

LOG_FILE_NAME=get_log_file_name()


os.makedirs(LOG_DIR,exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
filemode="w",
format='[%(asctime)s] %(name)s - %(levelname)s - %(message)s',
level=logging.INFO
)


def get_log_dataframe(file_path):
    data = []
    with open(file_path) as log_file:
        for line in log_file.readlines():
            data.append(line.split("^;"))

    log_df = pd.DataFrame(data)

    return log_df
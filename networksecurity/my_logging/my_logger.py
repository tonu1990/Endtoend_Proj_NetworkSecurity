import logging # this is a standard built-in library in Python
import os
from datetime import datetime 

# create Log file name format
LOG_FILE_NAME = f"{datetime.now().strftime('%Y_%m_%d__%HHr_%Mm_%Ss')}.log"
print(LOG_FILE_NAME)

# Create the log file save location 
log_dir_path = os.path.join(os.getcwd(), "Logs")
os.makedirs(log_dir_path, exist_ok=True)

LOG_FILE_PATH = os.path.join(log_dir_path, LOG_FILE_NAME)  # Complete file path
print(LOG_FILE_PATH)

logging.basicConfig(
    filename= LOG_FILE_PATH,
    level=logging.INFO,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s", 
)
import logging
import os


def get_logger(name, log_directory="D://Work_Items/Projects_Output/Fairmoney/Logs"):

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_file = os.path.join(log_directory, f"{name}.log")
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)

    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
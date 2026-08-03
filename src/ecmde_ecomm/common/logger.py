import logging
import os

from pyspark.sql import DataFrame
from datetime import datetime
from zoneinfo import ZoneInfo


class DataFrameLogger:
    @staticmethod
    def show(df: DataFrame, max_rows: int = 10):
        df.show(max_rows)

    @staticmethod
    def display(df: DataFrame):
        if "display" in globals():
            display(df)
        else:
            DataFrameLogger.show(df)

    @staticmethod
    def print(msg: str, end: str = "\r\n"):
        now_etc = datetime.now(ZoneInfo("America/New_York"))
        print(f"{now_etc} - {msg}", end=end)


class Py4JFilter(logging.Filter):
    def filter(self, record):
        return not ("py4j.clientserver" in record.name and record.levelname == "INFO")


class Logger:
    @staticmethod
    def logger(name: str):
        # Get the root logger
        root_logger = logging.getLogger()

        # Get log level from environment variable
        log_level_str = os.environ.get("__LOG_LEVEL", "INFO").upper()

        # Map string log levels to logging constants
        log_level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        # Set the log level (default to INFO if invalid level specified)
        log_level = log_level_map.get(log_level_str, logging.INFO)
        root_logger.setLevel(log_level)

        # If no handlers exist yet, add a basic console handler to the root logger
        if not root_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "{asctime}: {name}: {levelname}: {message} ({filename}:{lineno})",
                style="{",
            )
            handler.setFormatter(formatter)
            # Add the Py4J filter to the handler
            handler.addFilter(Py4JFilter())
            root_logger.addHandler(handler)

        # Get the named logger
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        return logger

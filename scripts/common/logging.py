import logging
from typing import Optional, Union
import sys


class ConfiguredLogger:
    """
    A class for creating and configuring a logger.

    Attributes:
    - name (Optional[str]): The name of the logger. If not provided, the root logger is used.
    - level (Optional[int]): The logging level to be set. Defaults to logging.INFO.
    - format_str (Optional[str]): The logging format string to be used. Defaults to None.
    - file_path (Optional[str]): The file path to write the logs to. If not provided, logs will be printed to console.

    Methods:
    - debug(message: Union[str, Exception], *args, **kwargs) -> None:
        Logs a debug message to the logger. Accepts optional arguments and keyword arguments for formatting.
    - info(message: Union[str, Exception], *args, **kwargs) -> None:
        Logs an informational message to the logger. Accepts optional arguments and keyword arguments for formatting.
    - warning(message: Union[str, Exception], *args, **kwargs) -> None:
        Logs a warning message to the logger. Accepts optional arguments and keyword arguments for formatting.
    - error(message: Union[str, Exception], *args, **kwargs) -> None:
        Logs an error message to the logger. Accepts optional arguments and keyword arguments for formatting.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        level: int = logging.DEBUG,
        format_str: Optional[str] = None,
        file_path: Optional[str] = None,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        formatter = logging.Formatter(format_str)
        if file_path:
            file_handler = logging.FileHandler(file_path)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        else:
            stream_handler = logging.StreamHandler(stream=sys.stdout)
            stream_handler.setLevel(level)
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)

    def debug(self, message: Union[str, Exception], *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)

    def info(self, message: Union[str, Exception], *args, **kwargs):
        self.logger.info(message, *args, **kwargs)

    def warning(self, message: Union[str, Exception], *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)

    def error(self, message: Union[str, Exception], *args, **kwargs):
        self.logger.error(message, *args, **kwargs)


class Logger(object):
    """
    A logging utility that provides a ConfiguredLogger class with customizable log levels,
    formatting, and output destinations.

    The Logger class provides a set of methods that correspond to the logging levels:
    - debug(message: Union[str, Exception], *args, **kwargs): Logs a debug message.
    - info(message: Union[str, Exception], *args, **kwargs): Logs an informational message.
    - warning(message: Union[str, Exception], *args, **kwargs): Logs a warning message.
    - error(message: Union[str, Exception], *args, **kwargs): Logs an error message.

    Example usage:
    from common.logging import Logger

    Logger.info("Application started")
    Logger.warning("Missing configuration file")
    Logger.error("An error occurred: division by zero")"""

    logger = ConfiguredLogger(
        name=__name__,
        level=logging.DEBUG,
        format_str="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    @classmethod
    def debug(cls, message: Union[str, Exception], *args, **kwargs):
        Logger.logger.debug(message, *args, **kwargs)

    @classmethod
    def info(cls, message: Union[str, Exception], *args, **kwargs):
        Logger.logger.info(message, *args, **kwargs)

    @classmethod
    def warning(cls, message: Union[str, Exception], *args, **kwargs):
        Logger.logger.warning(message, *args, **kwargs)

    @classmethod
    def error(cls, message: Union[str, Exception], *args, **kwargs):
        Logger.logger.error(message, *args, **kwargs)
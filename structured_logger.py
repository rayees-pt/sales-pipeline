import logging
import logging.config
import json
import configparser
from logging.handlers import TimedRotatingFileHandler

class StructuredLogger:
    def __init__(self, name, filename='pipeline.log'):
        # Load configuration
        config = configparser.ConfigParser()
        config.read('config.ini')  
        debug = config.getint('Debug', 'debug', fallback=0)
        # Logger setup
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

        # File Handler for logging to the file
        file_handler = TimedRotatingFileHandler(filename, when="midnight", interval=1, backupCount=7)
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Console Handler for debug mode
        if debug == 1:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)  
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        # Ensure log messages are not propagated to the root logger
        self.logger.propagate = False

    def info(self, message, **kwargs):
        self.logger.info(self._format_message(message, **kwargs))

    def warning(self, message, **kwargs):
        self.logger.warning(self._format_message(message, **kwargs))

    def error(self, message, **kwargs):
        self.logger.error(self._format_message(message, **kwargs))

    def _format_message(self, message, **kwargs):
        if kwargs:
            return f"{message} | {json.dumps(kwargs)}"
        return message


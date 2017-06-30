# logging
import logging, logging.handlers

# forcing ram cleaning
import gc
# enable full error tracing in responses
import traceback

class logee:

    global logger


    def __init__(self, log_file_name, app_name ):

        # logging configuration

        LOG_FILENAME = log_file_name # 'collaboration.log'
        # create logger
        self.logger = logging.getLogger(app_name)#"rtpa-collaboration-analysis-class Dbpedia")
        self.logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        consolehandler = logging.StreamHandler()
        # Add the log message handler to the logger
        logFilehandler = logging.handlers.RotatingFileHandler(
            LOG_FILENAME, maxBytes=20000000, backupCount=5)
        consolehandler.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        consolehandler.setFormatter(formatter)
        logFilehandler.setFormatter(formatter)
        # add ch to logger
        self.logger.addHandler(consolehandler)
        self.logger.addHandler(logFilehandler)

        # enabling garabage collector
        gc.enable()





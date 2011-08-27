"""
    File: collector.py
    Description: This module starts the reader/writer threads
"""


import logging, logging.config


severity_default = "DEBUG"
logging_path_default = "../logs/"
rotating_default = "midnight"
formatter_default = "%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"


class Collector:
    def __init__(self, conf, to_log=False):
        self.conf = conf
        self.to_log = to_log
        if to_log: 
            self.set_logging()
            self.logger.info("Collector instantiated with success.")

    def set_logging(self):
        try:
            self.logger = logging.getLogger()
            try:
                severity = self.conf.SEVERITY
            except AttributeError:
                severity = severity_default

            self.logger.setLevel(severity)

            try:
                logging_path = self.conf.LOGGING_PATH
            except AttributeError:
                logging_path = logging_path_default

            filename = logging_path + 'collector.log'

            try:
                rotating = self.conf.ROTATING
            except AttributeError:
                rotating = rotating_default

            log_handler = logging.handlers.TimedRotatingFileHandler(filename, 
                                                                    when=rotating)

            try:
                formatter = self.conf.FORMATTER
            except AttributeError:
                formatter = formatter_default

            formatter = logging.Formatter(formatter)
            log_handler.setFormatter(formatter)
            self.logger.addHandler(log_handler)
        except Exception, e:
            print e

    def start(self):
        if self.to_log:
            self.logger.info("Starting collector...")

        import Queue

        from reader.myreader import MyReader
        from writer.mywriter import MyWriter

        q = Queue.Queue(maxsize=1024)

        myreader = MyReader(queue=q)
        mywriter = MyWriter(queue=q)

        myreader.start()
        if self.to_log:
            self.logger.info("Reader started")

        mywriter.start()
        if self.to_log:
            self.logger.info("Writer started")

        if self.to_log:
            self.logger.info("Collector started.")

        while True:
            pass

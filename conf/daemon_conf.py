"""
    File: daemon_conf.py
    Description: Pycollector configuration file.
"""

#PID SETTINGS
#------------

#PID_PATH: LogCollector pid path/filename
PID_FILE_PATH="/tmp/pycollector.pid"


#APPLICATION LOG SETTINGS
#------------------------

#LOGS_PATH: sets where the logs will be located.
LOGS_PATH="/tmp"

#LOG_SEVERITY: changes the level of severity for all application logs created.
#Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_SEVERITY="DEBUG"

#LOG_FORMATTER: sets the format of application log lines.
#For a complete reference of options, see:
#http://docs.python.org/library/logging.html#logrecord-attributes
LOG_FORMATTER="%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"

#LOG_ROTATING: describes when the log files will be rotated.
#Options: 
#'S'	Seconds
#'M'	Minutes
#'H'	Hours
#'D'	Days
#'W'	Week day (0=Monday)
#'midnight'	Roll over at midnight
LOG_ROTATING="midnight"

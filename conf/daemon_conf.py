"""
    File: daemon_conf.py
    Description: LogCollector configuration file.
"""

#PID SETTINGS-----------------------------------------------

#PID_PATH: LogCollector pid path/filename
PID_PATH="/home/kaiser/workspace/log-collector/collector.pid"

#ACTIVEMQ SETTINGS-----------------------------------------------

#ACTIVEMQ_SERVER: activemq stomp server
ACTIVEMQ_SERVER="localhost"

#ACTIVEMQ_PORT: activemq stomp port
ACTIVEMQ_PORT=61613

#APPLICATION LOG SETTINGS-----------------------------------------------

#LOGGING_PATH: sets where the logs will be located.
LOGGING_PATH="/home/kaiser/workspace/log-collector/logs/"

#SEVERITY: changes the level of severity for all application logs created.
#Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
SEVERITY="DEBUG"

#FORMATTER: sets the format of application log lines.
#For a complete reference of options, see:
#http://docs.python.org/library/logging.html#logrecord-attributes
FORMATTER="%(asctime)s - %(filename)s (%(lineno)d) [(%(threadName)-10s)] %(levelname)s - %(message)s"

#ROTATING: describes when the log files will be rotated.
#Options: 
#'S'	Seconds
#'M'	Minutes
#'H'	Hours
#'D'	Days
#'W'	Week day (0=Monday)
#'midnight'	Roll over at midnight
ROTATING="midnight"

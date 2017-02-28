import logging
import datetime
from unipath import Path

class NewFileHandler(logging.FileHandler):
    """Update FileHandler to name logs the current date"""
    def __init__(self, path, mode = 'a'):
        # Get todays date
        today = datetime.date.today()
        year = today.year
        month = "%02d" % today.month
        day = "%02d" % today.day
        date = "%s-%s-%s" % (year, month, day)

        # Takes the provided path and appends the date as the log name
        filename = Path(path, "%s.log" % date).absolute()
        super(NewFileHandler,self).__init__(filename, mode)
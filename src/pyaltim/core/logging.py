

import logging
# pyaltim wide logger
altlogger=logging.getLogger("pyaltim")

ch = logging.StreamHandler()

# create formatter
formatter = logging.Formatter('%(name)s-%(levelname)s: %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
altlogger.addHandler(ch)


def debugging():
    return altlogger.getEffectiveLevel() == logging.DEBUG

def setInfoLevel():
    """Set logging level for both python and c++ to INFO severity"""
    altlogger.setLevel(logging.INFO)

def setDebugLevel():
    """Set logging level for both python and c++ to DEBUG severity"""
    altlogger.setLevel(logging.DEBUG)


def setWarningLevel():
    """Set logging level for both python and c++ to WARNING severity"""
    altlogger.setLevel(logging.WARNING)

def setErrorLevel():
    """Set logging level for both python and c++ to WARNING severity"""
    altlogger.setLevel(logging.ERROR)

setInfoLevel()

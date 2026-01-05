import logging

logger = logging.getLogger('log')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
#ch = logging.FileHandler('c:/tmp/my.log')
ch.setLevel(logging.DEBUG) # INFO
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

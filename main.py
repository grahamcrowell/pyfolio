__author__ = 'grahamcrowell'

# load database
from db_io import *
from price_io import *
from trans_io import *

init_db()

ts = load_trans_data()
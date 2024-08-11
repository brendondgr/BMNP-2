import sys
import os
from configparser import ConfigParser
from datetime import datetime, timedelta

# Add the path ./main/ to the sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'main'))
from bmnp import BMNP_Data

startdate = '2002-06-01'
enddate = datetime.today().strftime('%Y-%m-%d')

# Subtract 1 day from enddate to make it the day before.
enddate = datetime.strptime(enddate, '%Y-%m-%d') - timedelta(days=1)
enddate = enddate.strftime('%Y-%m-%d')
# print(f'End Date: {enddate}')

# Create an instance of BMNP_Data
download = True
database = False
singles = True
bulk = False

data = BMNP_Data(startdate, enddate, downloadnew=download, downloadtype='loop',
                    create_databases=database, delete_singles=singles, delete_bulk=bulk)
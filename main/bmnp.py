from configparser import ConfigParser
from os import path, listdir, remove
from pathlib import Path
from datetime import datetime, timedelta
from subprocess import run
import netCDF4 as nc
from numpy import where, zeros, around, sort, rot90, flip, shape
import os
from pandas import DataFrame

class BMNP_Download:
    def __init__(self, dates=[], type = 'loop', manually = False):        
        # Read config.ini file
        self.config = ConfigParser()
        self.config.read('config.ini')
        
        # Check if netrc exists
        self.netrc_check = False
        
        # Downloaded dates
        self.downloaded_dates = []
        
        # Read the config.ini file, specifically for [coordinates]
        # Read in the min_lat, max_lat, min_lon, max_lon
        self.min_lat = self.config['coordinates'].getfloat('min_lat')
        self.max_lat = self.config['coordinates'].getfloat('max_lat')
        self.min_lon = self.config['coordinates'].getfloat('min_lon')
        self.max_lon = self.config['coordinates'].getfloat('max_lon')
        
        # Directories
        self.download_dir = f"{self.config['folders']['nc_download']}"
        self.refined_dir = f"{self.config['folders']['nc_sst']}"
        self.refined_csv = f"{self.config['folders']['csv_sst']}"
        
        # Set temporary dates in the format YYYY-MM-DD
        self.date_list = dates
        
        if not manually:
            # Cycle through the dates list
            if type == 'loop':
                for date in self.date_list:
                    # Set the date
                    self.date = date
                    
                    # Set the command
                    self.command = self.setCommand(date)
                    
                    # Check to see if the .netrc file exists
                    if not self.netrc_check:
                        self.checkNetRC()
                    
                    # Go through the cycle of downloading the data.
                    self.downloadData(date, self.command)
                
            elif type == 'bulk':
                print(f'Not yet implemented. Please stick to "loop" download for now.')
    
    def setCommand(self, date):
        dataset = 'MUR-JPL-L4-GLOB-v4.1'
        command = [
            'podaac-data-downloader',
            '-c', dataset,
            '-d', self.download_dir,
            '--start-date', f'{date}T20:00:00Z',
            '--end-date', f'{date}T20:00:00Z'
        ]
        
        return command
    
    def checkNetRC(self):
        def getTime():
            now = datetime.now()
            now = now.strftime('%H:%M:%S')
            return now
        
        home = str(Path.home())

        # Within home, check to see if .netrc exists``
        if not path.exists(f'{home}/.netrc'):
            print(f"[{getTime()}] No .netrc file found in home directory.")
            username = input(f"[{getTime()}] Enter your EarthData Username: ")
            password = input(f"[{getTime()}] Enter your EarthData Password: ")
            with open(f'{home}/.netrc', 'w') as f:
                f.write(f'machine urs.earthdata.nasa.gov login {username} password {password}')
        else:
            print(f"[{getTime()}] .netrc file found in home directory.")
        
        # Set netrc_check to True
        self.netrc_check = True

    def downloadData(self, date, command):
        def getTime():
            now = datetime.now()
            now = now.strftime('%H:%M:%S')
            return now
        
        def FixData(date_sp, command_run):
            # Check to see if the directory is empty, if so, return None.
            if len(listdir(self.download_dir)) == 0:
                # Print a statement stating that no files were downloaded.
                print(f'[{getTime()}] No files were downloaded... Please ensure that the login credentials are correct.')
                
                # Print output of the command that was run.
                # print(f'[{getTime()}] Command: {command_run}')
                
                return False
            else:
                # Print a statement stating that the files were downloaded.
                print(f'[{getTime()}] Files for the date {date_sp} were downloaded. Now fixing the data...')
                
                # Add date to the downloaded_dates list
                self.downloaded_dates.append(date_sp)
            
            # Check the directory and delete items that have ".txt" extensions
            for filename in listdir(self.download_dir):
                if filename.endswith('.txt'):
                    remove(f'{self.download_dir}{filename}')
            
            # Open the only other item which is an .nc file
            filename = listdir(self.download_dir)[0]
            file = nc.Dataset(self.download_dir + filename, 'r')
            
            # Filter the data to the area of interest.
            lons = file.variables['lon'][:]
            lats = file.variables['lat'][:]
            temp = file.variables['analysed_sst'][:]
            
            lon_idx = where((lons >= self.min_lon) & (lons <= self.max_lon))[0]
            lat_idx = where((lats >= self.min_lat) & (lats <= self.max_lat))[0]
            
            lons = lons[lon_idx]
            lats = lats[lat_idx]
            temp = temp[:, lat_idx, :][:, :, lon_idx]
            
            # Take new "temp" data, convert to csv using pandas dataframe, and save it to refined_csv directory.
            df = DataFrame(temp[0], index=lats, columns=lons)
            df.to_csv(f'{self.refined_csv}{date_sp}.csv')
            
            # Create a new netCDF file with the filtered data
            new_file = nc.Dataset(f'{self.refined_dir}{date_sp}.nc', 'w')
            
            # Create dimensions
            new_file.createDimension('lon', len(lons))
            new_file.createDimension('lat', len(lats))
            
            #  Create variables
            new_lons = new_file.createVariable('lon', 'f4', ('lon',))
            new_lats = new_file.createVariable('lat', 'f4', ('lat',))
            new_temp = new_file.createVariable('analysed_sst', 'f4', ('lat', 'lon'))
            
            # Add attributes
            new_lons.units = 'degrees_east'
            new_lats.units = 'degrees_north'
            new_temp.units = 'kelvin'
            
            # Add data
            new_lons[:] = lons
            new_lats[:] = lats
            new_temp[:] = temp
            
            # Close the files
            file.close()
            new_file.close()
            
            # Remove the old file
            remove(self.download_dir + filename)
            
            return True
        
        # Run the command
        print(f'[{getTime()}] Downloading the Date: {date}. Please give this a moment...')
        
        # Run subprocess run
        command_run = run(command, capture_output=True, text=True)
        
        # Run the FixData Function
        day_downloaded = FixData(date, command_run)
        
        # Print new statement stating it was downloaded.
        if day_downloaded: print(f'[{getTime()}] Date: {date} has been downloaded and refined.')

class BMNP_Data:
    def __init__(self, startdate, enddate, downloadnew = False, downloadtype = 'loop', create_databases = False, delete_singles = False, delete_bulk = False, manually = False):
        # Read config.ini file
        self.config = ConfigParser()
        self.config.read('config.ini')
        
        # Directories
        self.download_dir = f"{self.config['folders']['nc_download']}"
        self.refined_dir = f"{self.config['folders']['nc_sst']}"
        self.data_dir = f"{self.config['folders']['data']}"
        self.csv_sst = f"{self.config['folders']['csv_sst']}"
        self.nc_dhw = f"{self.config['folders']['nc_dhw']}"
        self.csv_dhw = f"{self.config['folders']['csv_dhw']}"
        
        # NOTE: downloadtype is either loop or bulk. Loop will download each file individually. Bulk will download all files at once.
        # Get the start end dates
        self.start_date = self.changeDateLayout(startdate)
        self.end_date = self.changeDateLayout(enddate)
        
        # Get other parameters
        self.downloadnew = downloadnew
        self.downloadtype = downloadtype
        self.create_databases = create_databases
        self.delete_singles = delete_singles
        self.delete_bulk = delete_bulk
        
        # Make list of dates between start and end date
        self.dates = self.makeDateList()
        
        # List of missing dates from 2002-06-01 to today.
        self.missing_dates = self.checkMissingDates()
        print(f'[{self.getHrMnSc()}] Dates Missing from SST Database: {self.missing_dates}')
        
        # Dates Missing boolean
        self.dates_missing = True
        
        # From missing dates, check to see what dates in self.dates are missing.
        # if type(self.missing_dates) != None: self.missing_dates = [date for date in self.dates if date in self.missing_dates]
        # If self.missing_dates is not empty:
        if len(self.missing_dates) > 0:
            # Create instance of BMNP_Download here
            if self.downloadnew:
                print(f'[{self.getHrMnSc()}] Setting Up the Download for Missing Dates...')
                self.download = BMNP_Download(dates=self.missing_dates, type=self.downloadtype)
        else:
            self.dates_missing = False
        
        if manually:
            # Create a self.download object.
            self.download = BMNP_Download(dates=self.dates, type=self.downloadtype, manually=True)
        
        #################################################
        ##             Database Creation               ##
        #################################################
        # if self.create_databases:
        #     # Run the database function
        #     self.databaseSetup(self.missing_dates)
            
        #     # Run the database reordering function.
        #     print(f'[{self.getHrMnSc()}] Reordering the database...')
        #     self.databaseReorder()
            
        #     # Now we need to remake the DHW data into a new netCDF file.
        #     print(f'[{self.getHrMnSc()}] Calculating DHW...')
        #     self.dhwDatabase(self.missing_dates)
        # else:
        #     print(f'[{self.getHrMnSc()}] Database creation is turned off. Please turn on to create the databases.')
        
        # Delete the single files in the csv_sst, csv_dhw and nc_dhw folders.
        if self.delete_singles and not manually:
            print(f'[{self.getHrMnSc()}] Deleting non-downloadable single-day files...')
            # for file in listdir(self.csv_sst):
            #     remove(f'{self.csv_sst}{file}')
            for file in listdir(self.csv_dhw):
                remove(f'{self.csv_dhw}{file}')
            for file in listdir(self.nc_dhw):
                remove(f'{self.nc_dhw}{file}')
        
        # Create individual DHW files
        if not manually:
            print(f'[{self.getHrMnSc()}] Creating DHW files...')
            self.createDHWs()
        
        # Create CSV files from the refined netCDF files.
        if not manually: self.recreateCSVs()
        
        # Print a statement stating that everything is in order.
        if not manually:
            print(f'[{self.getHrMnSc()}] Everything is in order.')
    
    def getHrMnSc(self):
        now = datetime.now()
        return now.strftime('%H:%M:%S')
    
    def checkMissingDates(self):
        # Make a list of items of refined nc files.
        files = listdir(f'{self.refined_dir}')
        
        # Make a list of "start" and "end", which the start and end are self.start_date and self.end_date
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        all_dates = [start + timedelta(n) for n in range(int((end-start).days)+1)]
        all_dates = [date.strftime('%Y-%m-%d') for date in all_dates]
        
        # Check for missing dates
        missing_dates = []
        
        # Cycle through all_dates. Check to see if it is in "files". If not, add to missing_dates.
        for date in all_dates:
            if f'{date}.nc' not in files:
                missing_dates.append(date)
        
        return missing_dates
    
    def makeDateList(self):
        # Make list of dates from self.start_date to self.end_date
        start = datetime.strptime(self.start_date, '%Y-%m-%d')
        end = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        # Make list of dates between start and end date
        dates = [start + timedelta(n) for n in range(int((end-start).days)+1)]
        
        # Convert every date to format YYYY-MM-DD
        dates = [date.strftime('%Y-%m-%d') for date in dates]
        
        return dates
    
    def changeDateLayout(self, date):
        def printStatement(date, nd=False):
            if nd:
                print(f"[{self.getHrMnSc()}] A proper date format was not found for your input '{date}'. This date will be marked as 'nd'. Please reformat to YYYY-MM-DD.")
            else:
                print(f"[{self.getHrMnSc()}] Date format not YYYY-MM-DD (Your date is: '{date}'). This was changed to YYYY-MM-DD. Please use this format in the future.")
        # Check to see if the date contains '-'
        if '-' in date:
            # Check to see if the date is in format other than YYYY-MM-DD, correct if so:
            if len(date) == 8:
                year = date[0:4]
                month = date[4:6]
                day = date[6:8]
                printStatement(date)
                return f'{year}-{month}-{day}'
            elif len(date) == 10:
                # Check for various composition of date formats.
                if date[4] == '-' and date[7] == '-':
                    return date
                elif date[2] == '-':
                    year = date[6:10]
                    month = date[0:2]
                    day = date[3:5]
                    printStatement(date)
                    return f'{year}-{month}-{day}'
        # Repeat for '/' separator
        elif '/' in date:
            if len(date) == 8:
                year = date[4:8]
                month = date[0:2]
                day = date[3:5]
                printStatement(date)
                return f'{year}-{month}-{day}'
            elif len(date) == 10:
                if date[4] == '/' and date[7] == '/':
                    year = date[6:10]
                    month = date[0:2]
                    day = date[3:5]
                    printStatement(date)
                    return f'{year}-{month}-{day}'
        # If date is in format YYYYMMDD, convert
        elif len(date) == 8:
            year = date[0:4]
            month = date[4:6]
            day = date[6:8]
            printStatement(date)
            return f'{year}-{month}-{day}'
        else:
            printStatement(date, nd=True)
            return 'nd'

    def databaseSetup(self, missing_dates):
        def dateSince(date):
            date = datetime.strptime(date, '%Y-%m-%d')
            
            # Calculate the number of days since 1981-01-01
            since = date - datetime(1981, 1, 1)
            
            return since.days
        
        # Check to see if the database and nc files named the following are in the config['folders']['data'] directory
        # 1. sst_bmnp.nc
        data_folder = listdir(f"{self.config['folders']['data']}")
        
        # Checks, True/False for each:
        # db_exists = 'bmnp.db' in data
        nc_exists = 'sst_bmnp.nc' in data_folder
        
        if nc_exists:
            nc_accumulated = nc.Dataset(f'{self.config["folders"]["data"]}sst_bmnp.nc', 'r')
            
            # Check to see if the "time" variable exists, if not, set nc_exists to False and delete that file.
            if 'time' not in nc_accumulated.variables:
                print(f'[{self.getHrMnSc()}] The ".nc" file is not correctly formatted. Deleting and creating a new one.')
                nc_exists = False
                remove(f'{self.config["folders"]["data"]}sst_bmnp.nc')
        
        # If nc_exists, open the file and check the last date.
        if nc_exists:
            # Get the last date in nc_accumulated.
            last_date_nc = nc_accumulated.variables['time'][-1]
            last_date_nc = datetime(1981, 1, 1) + timedelta(days=int(last_date_nc))
            last_date_nc = last_date_nc.strftime('%Y-%m-%d')
            
            # Find the last date in the refined folder
            last_date_acc = listdir(self.refined_dir)[-1][0:10]
            
            # Check to see if the last date is the same as the last date in the refined folder.
            if last_date_nc == last_date_acc or len(missing_dates) == 0:
                print(f'[{self.getHrMnSc()}] The ".nc" file with accumulated data is up to date. No new data is needed.')
            else:
                # Calculate the number of days between the last date in nc and the last date in the refined folder.
                last_date_nc = datetime.strptime(last_date_nc, '%Y-%m-%d')
                last_date_acc = datetime.strptime(last_date_acc, '%Y-%m-%d')
                days = (last_date_acc - last_date_nc).days
                
                # Check to see if the number of days is greater than 0.
                if days > 0:
                    # Print message stating that the nc file needs to be updated.
                    print(f'[{self.getHrMnSc()}] The ".nc" file with accumulated data is not up to date. A total of {days} files need to be added to the database.')
                    
                    # Take the last number of "days" in the last_refined files and add them to the nc file.
                    if len(missing_dates) > 0:
                        for idx, file in enumerate(files[-days:]):
                            # Open the file
                            data = nc.Dataset(self.refined_dir + file, 'r')
                            
                            # Retrieve the data
                            lons = data.variables['lon'][:]
                            lats = data.variables['lat'][:]
                            temp = data.variables['analysed_sst'][:]
                            time = data.variables['time'][:]
                            
                            # Add the data to the nc file
                            nc_accumulated.variables['analysed_sst'][idx] = temp - 273.14
                            nc_accumlation.variables['time'][idx] = dateSince(file[0:10])
                            
                            # Close the file
                            data.close()
                            
                            # If the index is divisible by 50, print a message stating that the file is being added, out of the total.
                            if idx % 50 == 0 and idx != 0:
                                print(f'[{self.getHrMnSc()}] File [{idx} / {days}] is being added to the database.')
                else:
                    # Print that we need to add specific dates to the nc file.
                    print(f'[{self.getHrMnSc()}] The ".nc" file with accumulated data is not up to date. Specific dates need to be added to the database.')
                    
                    # We need to add specific dates to the nc file.
                    for date in missing_dates:
                        # Open the file
                        data = nc.Dataset(self.refined_dir + date + '.nc', 'r')
                        
                        # Retrieve the data
                        lons = data.variables['lon'][:]
                        lats = data.variables['lat'][:]
                        temp = data.variables['analysed_sst'][:]
                        time = data.variables['time'][:]
                        
                        # Add the data to the nc file
                        nc_accumulated.variables['analysed_sst'][idx] = temp - 273.14
                        nc_accumlation.variables['time'][idx] = dateSince(date)
                        
                        # Close the file
                        data.close()
                        
                        # If the index is divisible by 50, print a message stating that the file is being added, out of the total.
                        if idx % 50 == 0 and idx != 0:
                            print(f'[{self.getHrMnSc()}] File [{idx} / {len(missing_dates)}] is being added to the database.')
                
                print(f'[{self.getHrMnSc()}] All files have been added to the database.')
                # Close the nc file
                nc_accumlation.close()
        else:
            # Print message stating that the nc file does not exist and that it will be created.
            print(f'[{self.getHrMnSc()}] No ".nc" file with accumulated data was found. A new file will be created in the directory "{self.config["folders"]["data"]}" called "sst_bmnp.nc".')
            
            # We need to create the nc file that is an accumulation of all nc files in config['folders']['nc_sst']
            nc_accumlation = nc.Dataset(f"{self.config['folders']['data']}sst_bmnp.nc", 'w')
            
            # Create a list of files in the nc_sst directory
            files = listdir(self.refined_dir)
            
            # Print a messages stating how many files there are. If there is over 1,000 files, then state there are many files and this may take some time.
            if len(files) > 1000:
                print(f'[{self.getHrMnSc()}] There are {len(files)} files that need to be added. This may take some time.')
            
            # Cycle through the files and add them to the nc_accumlation file, with their respective dates.
            # For the first files, create dimensions using the the variable "lat" and "lon" from the file.
            # "analysed_sst" will be the data. Subtract 273.14 from the data to convert to Celsius.
            # Also, using datetime library, convert retrieved date to a number of days since 1981-01-01 00:00:00
            # Add the data to the nc_accumlation file.
            for idx, file in enumerate(files):
                # Open the file
                data = nc.Dataset(self.refined_dir + file, 'r')
                
                # Get the data
                lons = data.variables['lon'][:]
                lats = data.variables['lat'][:]
                temp = data.variables['analysed_sst'][:]
                time = data.variables['time'][:]
                
                # Create the dimensions
                if idx == 0:
                    nc_accumlation.createDimension('lon', len(lons))
                    nc_accumlation.createDimension('lat', len(lats))
                    nc_accumlation.createDimension('time', None)
                    
                    # Create the variables
                    nc_lons = nc_accumlation.createVariable('lon', 'f4', ('lon',))
                    nc_lats = nc_accumlation.createVariable('lat', 'f4', ('lat',))
                    nc_time = nc_accumlation.createVariable('time', 'f4', ('time',))
                    nc_temp = nc_accumlation.createVariable('analysed_sst', 'f4', ('time', 'lat', 'lon'))
                    
                    # Add attributes
                    nc_lons.units = 'degrees_east'
                    nc_lats.units = 'degrees_north'
                    nc_time.units = 'days since 1981-01-01 00:00:00'
                    nc_temp.units = 'celsius'
                    
                    # Add data
                    nc_lons[:] = lons
                    nc_lats[:] = lats
                    nc_time[:] = dateSince(file[0:10])
                    nc_temp[:] = temp - 273.14
                else:
                    # Add the data to the nc file
                    nc_accumlation.variables['analysed_sst'][idx] = temp - 273.14
                    nc_accumlation.variables['time'][idx] = dateSince(file[0:10])
                
                # If the index is divisible by 250, print a message stating that the file is being added, out of the total.
                lof = len(files)
                if lof > 5000:
                    if idx % 1000 == 0:
                        print(f'[{self.getHrMnSc()}] File [{idx} / {lof}] is being added to the database.')
                elif lof > 1000:
                    if idx % 250 == 0:
                        print(f'[{self.getHrMnSc()}] File [{idx} / {lof}] is being added to the database.')
                else:
                    if idx % 50 == 0:
                        print(f'[{self.getHrMnSc()}] File [{idx} / {lof}] is being added to the database.')
                
                # Close the file
                data.close()
            
            # Print a message stating that all files have been added to the database.
            print(f'[{self.getHrMnSc()}] All files have been added to the database.')
            
            # Close the nc file
            nc_accumlation.close()

    def databaseReorder(self):
        # Open the nc file containing all of the data.
        nc_file = nc.Dataset(f'{self.config["folders"]["data"]}sst_bmnp.nc', 'r')
        
        # Sort the data by time in ascending order.
        time = nc_file.variables['time'][:]
        time_sort = time.argsort()
        
        # Create a temp directory in the data folder, if it does not exist, using the os library.
        if not path.exists(f'{self.config["folders"]["data"]}/temp'): os.mkdir(f'{self.config["folders"]["data"]}/temp')
        
        # Create a new nc file with the sorted data.
        nc_file_sorted = nc.Dataset(f'{self.config["folders"]["data"]}/temp/sst_bmnp.nc', 'w')
        
        # Create dimensions
        nc_file_sorted.createDimension('lon', len(nc_file.variables['lon'][:]))
        nc_file_sorted.createDimension('lat', len(nc_file.variables['lat'][:]))
        nc_file_sorted.createDimension('time', len(time))
        
        # Create variables
        nc_lons = nc_file_sorted.createVariable('lon', 'f4', ('lon',))
        nc_lats = nc_file_sorted.createVariable('lat', 'f4', ('lat',))
        nc_time = nc_file_sorted.createVariable('time', 'f4', ('time',))
        nc_temp = nc_file_sorted.createVariable('analysed_sst', 'f4', ('time', 'lat', 'lon'))
        
        # Add attributes
        nc_lons.units = 'degrees_east'
        nc_lats.units = 'degrees_north'
        nc_time.units = 'days since 1981-01-01 00:00:00'
        nc_temp.units = 'celsius'
        
        # Add data
        nc_lons[:] = nc_file.variables['lon'][:]
        nc_lats[:] = nc_file.variables['lat'][:]
        nc_time[:] = time
        nc_temp[:] = nc_file.variables['analysed_sst'][time_sort]
        
        # Close the files so that the sst_bmnp.nc can be deleted and replaced with the one in temp.
        nc_file.close()
        nc_file_sorted.close()
        
        # Deleted the bmnp.nc file in data folder, then move the one in temp to the data folder. Then delete temp directory
        remove(f'{self.config["folders"]["data"]}sst_bmnp.nc')
        os.rename(f'{self.config["folders"]["data"]}/temp/sst_bmnp.nc', f'{self.config["folders"]["data"]}sst_bmnp.nc')
        os.rmdir(f'{self.config["folders"]["data"]}/temp')

    def dhwDatabase(self, missing_dates):
        def dateSince(date):
            date = datetime.strptime(date, '%Y-%m-%d')
            
            # Calculate the number of days since 1981-01-01
            since = date - datetime(1981, 1, 1)
            
            return since.days
        
        # Check to see if "dhw_bmnp.nc" exists in the data folder.
        data_folder = listdir(self.data_dir)
        dhw_exists = 'dhw_bmnp.nc' in data_folder
        hrcs_exists = 'hrcs_mmm.nc' in data_folder
        
        # Open the nc file in data called "hrcs_mmm.nc" ... as long as it exists. if it doesn't exist, print a message and return None.
        if not hrcs_exists:
            print(f'[{self.getHrMnSc()}] The file "hrcs_mmm.nc" does not exist in the data folder. This file is used as the bleaching threshold for the DHW calculation. Please add this file to the {self.data_dir} folder.')
            return None
        else:
            hrcs = nc.Dataset(f'{self.data_dir}hrcs_mmm.nc', 'r')
        
        # If it doesn't exists, we need to open the nc file sst_bmnp.nc to calculate the DHW.
        # If it does exist, we need to calculate the DHW for the missing dates.
        delete_dhw = True
        if dhw_exists and delete_dhw:
            #  Delete the dhw_bmnp.nc file
            remove(f'{self.data_dir}dhw_bmnp.nc')
            print(f'[{self.getHrMnSc()}] The file "dhw_bmnp.nc" exists in the data folder. This file will be deleted and recreated.')
        
        # Add 1 to the hrcs file to make bleaching_threshold
        bleaching_threshold = hrcs.variables['variable'][:] + 0.6
        
        # Print the bleaching threshold
        # print(f'[{self.getHrMnSc()}] Bleaching Threshold: {bleaching_threshold}')
        
        # Load the sst_bmnp.nc file
        sst = nc.Dataset(f'{self.data_dir}sst_bmnp.nc', 'r')
        
        # Sort the sst file by time in ascending order.
        time = sst.variables['time'][:]
        time_sort = sort(time)
        
        # Gather the lat and lon in both hrcs and sst files, rounding each item in list to 2 decimal places.
        hrcs_lat = hrcs.variables['lat'][:]
        hrcs_lon = hrcs.variables['lon'][:]
        sst_lat = sst.variables['lat'][:]
        sst_lon = sst.variables['lon'][:]
        hrcs_lat = [round(lat, 2) for lat in hrcs_lat]
        hrcs_lon = [round(lon, 2) for lon in hrcs_lon]
        sst_lat = [round(lat, 2) for lat in sst_lat]
        sst_lon = [round(lon, 2) for lon in sst_lon]
        
        # Give me the minimum and maximum in both lat and lon for the hrcs file
        hrcs_min_lat = min(hrcs_lat)
        hrcs_max_lat = max(hrcs_lat)
        hrcs_min_lon = min(hrcs_lon)
        hrcs_max_lon = max(hrcs_lon)
        
        # Based on the min and max lat and lon, find the indices in the sst file.
        min_lat_idx = where(sst_lat == hrcs_min_lat)[0][0]
        max_lat_idx = where(sst_lat == hrcs_max_lat)[0][0]+1
        min_lon_idx = where(sst_lon == hrcs_min_lon)[0][0]
        max_lon_idx = where(sst_lon == hrcs_max_lon)[0][0]+1
        
        # Create a list of new lat and lons based on the min and max lat and lon indices FROM SST FILE
        new_lat = sst_lat[min_lat_idx:max_lat_idx]
        new_lon = sst_lon[min_lon_idx:max_lon_idx]
        
        # Create a new nc file called "dhw_bmnp.nc" in the data folder.
        dhw = nc.Dataset(f'{self.data_dir}dhw_bmnp.nc', 'w')
        
        # Create dimensions
        dhw.createDimension('lon', len(new_lon))
        dhw.createDimension('lat', len(new_lat))
        dhw.createDimension('time', None)
        
        # Create variables
        dhw_lons = dhw.createVariable('lon', 'f4', ('lon',))
        dhw_lats = dhw.createVariable('lat', 'f4', ('lat',))
        dhw_time = dhw.createVariable('time', 'f4', ('time',))
        dhw_dhw = dhw.createVariable('dhw', 'f4', ('time', 'lat', 'lon'))
        
        # Add attributes
        dhw_lons.units = 'degrees_east'
        dhw_lats.units = 'degrees_north'
        dhw_time.units = 'days since 1981-01-01 00:00:00'
        dhw_dhw.units = 'degree heating weeks'
        
        # Add data
        dhw_lons[:] = new_lon
        dhw_lats[:] = new_lat
        
        # Cycle through each dame in time_sort.
        for idx, date in enumerate(time_sort):
            # If the date is one of the first 84 days, skip the iteration.
            if idx < 84: continue
            
            # Create an np array of zeros, with the shape of the new_lat and new_lon
            total_dhw = zeros((len(new_lat), len(new_lon)))
            
            date = 0
            
            for i in range(84):
                # Get the idx - i date in time_sort
                date = time_sort[idx-i]
                
                # Find idx of "date" in "time" variable for sst file
                date_idx = where(sst.variables['time'][:] == date)[0][0]
                
                # Load the sst data for the date
                sst_data = sst.variables['analysed_sst'][date_idx, min_lat_idx:max_lat_idx, min_lon_idx:max_lon_idx]
                
                # Do sst_date - bleaching_threshold
                sst_data_dailydhw = sst_data - bleaching_threshold
                
                # Replace all negative values with 0
                sst_data_dailydhw = where(sst_data_dailydhw < 0, 0, sst_data_dailydhw)
            
            # Add the daily dhw to the total dhw
            total_dhw += sst_data_dailydhw
            
            # Round the total_dhw to 2 decimal places
            total_dhw = around(total_dhw, 2)
            
            # Add the total_dhw to the dhw file, at idx associated with date.
            dhw_dhw[int(idx)] = total_dhw
            
            if idx % 250 == 0:
                print(f'[{self.getHrMnSc()}] DHW completed for [{idx} / {len(time_sort)}] days.')
                
        # Close the files
        dhw.close()
        sst.close()
        hrcs.close()

    def createDHWs(self):
        def dateSince(date):
            date = datetime.strptime(date, '%Y-%m-%d')
            
            # Calculate the number of days since 1981-01-01
            since = date - datetime(1981, 1, 1)
            
            return since.days
        
        # Create a list of items that are in nc_sst directory.
        files = listdir(f"{self.refined_dir}")
        
        # Order files by date
        files = [file[0:10] for file in files]
        files = list(set(files))
        files = sort(files)
        files_nc = [f'{file}.nc' for file in files]
        
        # Load first file to get lat and lon
        data = nc.Dataset(f"{self.refined_dir}{files_nc[0]}", 'r')
        
        # Get thge data
        lons = data.variables['lon'][:]
        lats = data.variables['lat'][:]
        
        # Add 1 to the hrcs file to make bleaching_threshold
        hrcs = nc.Dataset(f'{self.data_dir}hrcs_mmm.nc', 'r')
        bleaching_threshold = hrcs.variables['variable'][:] + 1.0
        
        # Gather the lat and lon in both hrcs and sst files, rounding each item in list to 2 decimal places.
        hrcs_lat = hrcs.variables['lat'][:]
        hrcs_lon = hrcs.variables['lon'][:]
        sst_lat = lats
        sst_lon = lons
        hrcs_lat = [round(lat, 2) for lat in hrcs_lat]
        hrcs_lon = [round(lon, 2) for lon in hrcs_lon]
        sst_lat = [round(lat, 2) for lat in sst_lat]
        sst_lon = [round(lon, 2) for lon in sst_lon]
        
        # Order in ascending order
        hrcs_lat = sort(hrcs_lat)
        hrcs_lon = sort(hrcs_lon)
        sst_lat = sort(sst_lat)
        sst_lon = sort(sst_lon)
        
        # Give me the minimum and maximum in both lat and lon for the hrcs file
        hrcs_min_lat = min(hrcs_lat)
        hrcs_max_lat = max(hrcs_lat)
        hrcs_min_lon = min(hrcs_lon)
        hrcs_max_lon = max(hrcs_lon)
        
        # Based on the min and max lat and lon, find the indices in the sst file.
        min_lat_idx = where(sst_lat == hrcs_min_lat)[0][0]
        max_lat_idx = where(sst_lat == hrcs_max_lat)[0][0]+1
        min_lon_idx = where(sst_lon == hrcs_min_lon)[0][0]
        max_lon_idx = where(sst_lon == hrcs_max_lon)[0][0]+1
        
        # Print the indices:
        # print(f'[{self.getHrMnSc()}] Min Lat Idx: {min_lat_idx}')
        # print(f'[{self.getHrMnSc()}] Max Lat Idx: {max_lat_idx}')
        # print(f'[{self.getHrMnSc()}] Min Lon Idx: {min_lon_idx}')
        # print(f'[{self.getHrMnSc()}] Max Lon Idx: {max_lon_idx}')
        
        # Create a list of new lat and lons based on the min and max lat and lon indices FROM SST FILE
        new_lat = sst_lat[min_lat_idx:max_lat_idx]
        new_lon = sst_lon[min_lon_idx:max_lon_idx]
        
        # Close data
        data.close()
        
        # Cycle through files, with idx and file name
        if self.delete_singles:
            for idx, file in enumerate(files_nc):
                # Check to see if the file is already in the nc_dhw directory. If so, skip the iteration. (Also skip if first 84)
                if path.exists(f"{self.config['folders']['nc_dhw']}{file[0:10]}.nc") or (idx < 84): continue
                
                # Check to see if the file has "2010" in its name, if not, skip.
                # if '2010' not in file: continue
                
                # Triggers date_change to True, so that the date is changed to the date of the file.
                date_change = True
                date_name = ""
                
                # Loop through 84 days, starting from the current idx
                for i in range(84):
                    # Get the idx - i date in files
                    date = files_nc[idx-i]
                    
                    # Subtract i from idx to get the date
                    if date_change:
                        date_name = date[0:10]
                        date_change = False
                    
                    # Open the file
                    data = nc.Dataset(f"{self.config['folders']['nc_sst']}{date}", 'r')
                    
                    # Get sst data from the file, with the new lat and lon indices
                    sst_data = data.variables['analysed_sst'][0, min_lat_idx:max_lat_idx, min_lon_idx:max_lon_idx] - 273.14
                    
                    # Do sst_data - bleaching_threshold
                    sst_data_dailydhw = sst_data - bleaching_threshold
                    
                    # Replace all negative values with 0
                    sst_data_dailydhw = where(sst_data_dailydhw < 0, 0, sst_data_dailydhw)
                    
                    # Add the daily dhw to the total dhw
                    if i == 0:
                        total_dhw = sst_data_dailydhw
                    else:
                        total_dhw += sst_data_dailydhw
                
                # Round the total_dhw to 2 decimal places
                total_dhw = around(total_dhw, 2)
                
                # Flip total_dhw over the y=x axis
                total_dhw = flip(total_dhw, 0)
                
                # Create a new nc file in the nc_dhw directory, with the name of the date.
                dhw = nc.Dataset(f"{self.config['folders']['nc_dhw']}{date_name}.nc", 'w')
                
                # Create dimensions
                dhw.createDimension('lon', len(new_lon))
                dhw.createDimension('lat', len(new_lat))
                
                # Create variables
                dhw_lons = dhw.createVariable('lon', 'f4', ('lon',))
                dhw_lats = dhw.createVariable('lat', 'f4', ('lat',))
                dhw_dhw = dhw.createVariable('dhw', 'f4', ('lat', 'lon'))
                
                # Add attributes
                dhw_lons.units = 'degrees_east'
                dhw_lats.units = 'degrees_north'
                dhw_dhw.units = 'degree heating weeks'
                
                # Add data
                dhw_lons[:] = new_lon
                dhw_lats[:] = new_lat
                dhw_dhw[:] = total_dhw
                
                # Create a csv file from the dhw file, with the lat and lon included in the index and columns.
                df = DataFrame(total_dhw, index=new_lat, columns=new_lon)
                df.to_csv(f"{self.config['folders']['csv_dhw']}{date_name}.csv")
                
                # Close the files
                dhw.close()
                data.close()
                
                # If the file is divisible by 250, print a message stating that the file is being added, out of the total.
                if idx % 1000 == 0:
                    print(f'[{self.getHrMnSc()}] File [{idx} / {len(files)}] has been created for single-day DHWs.')
        elif self.dates_missing and not self.delete_singles:
            # Cycle through each of the self.downlod.downloaded_dates and do the same as above, but for the missing dates.
            for idx, date in enumerate(self.download.downloaded_dates):
                # Find idx of the date in files list
                date_idx = where(files == date)[0][0]
                # Print idx
                print(f'[{self.getHrMnSc()}] Date Index: {date_idx}')
                
                # If this date is in the first 84 days, skip the iteration.
                if date_idx < 84: continue
                
                # Triggers date_change to True, so that the date is changed to the date of the file.
                date_change = True
                date_name = ""
                
                # Loop through 84 days, starting from the current idx
                for i in range(84):
                    # Get the idx - i date in files_nc
                    date = files[date_idx-i]
                    
                    # Subtract i from idx to get the date
                    if date_change:
                        date_name = date
                        date_change = False
                    
                    # Open the file
                    data = nc.Dataset(f"{self.config['folders']['nc_sst']}{date}.nc", 'r')
                    
                    # Get sst data from the file, with the new lat and lon indices
                    sst_data = data.variables['analysed_sst'][0, min_lat_idx:max_lat_idx, min_lon_idx:max_lon_idx] - 273.14
                    
                    # Do sst_data - bleaching_threshold
                    sst_data_dailydhw = sst_data - bleaching_threshold
                    
                    # Replace all negative values with 0
                    sst_data_dailydhw = where(sst_data_dailydhw < 0, 0, sst_data_dailydhw)
                    
                    # Add the daily dhw to the total dhw
                    if i == 0:
                        total_dhw = sst_data_dailydhw
                    else:
                        total_dhw += sst_data_dailydhw
                
                # Round the total_dhw to 2 decimal places
                total_dhw = around(total_dhw, 2)
                
                # Flip total_dhw over the y=x axis
                total_dhw = flip(total_dhw, 0)
                
                # Create a new nc file in the nc_dhw directory, with the name of the date.
                dhw = nc.Dataset(f"{self.config['folders']['nc_dhw']}{date_name}.nc", 'w')
            
                # Create dimensions
                dhw.createDimension('lon', len(new_lon))
                dhw.createDimension('lat', len(new_lat))
                
                # Create variables
                dhw_lons = dhw.createVariable('lon', 'f4', ('lon',))
                dhw_lats = dhw.createVariable('lat', 'f4', ('lat',))
                dhw_dhw = dhw.createVariable('dhw', 'f4', ('lat', 'lon'))
                
                # Add attributes
                dhw_lons.units = 'degrees_east'
                dhw_lats.units = 'degrees_north'
                dhw_dhw.units = 'degree heating weeks'
                
                # Add data
                dhw_lons[:] = new_lon
                dhw_lats[:] = new_lat
                dhw_dhw[:] = total_dhw
                
                # Create a csv file from the dhw file, with the lat and lon included in the index and columns.
                df = DataFrame(total_dhw, index=new_lat, columns=new_lon)
                df.to_csv(f"{self.config['folders']['csv_dhw']}{date_name[0:10]}.csv")
                
                # Close the files
                dhw.close()
                data.close()
                
                # If the length of self.download.downloaded_dates is less than 25, print every file that is created.
                if len(self.download.downloaded_dates) < 25:
                    print(f'[{self.getHrMnSc()}] The date {date_name[0:10]} has been added to single-day DHW files.')
                # Otherwise print every 100.
                elif idx % 100 == 0:
                    print(f'[{self.getHrMnSc()}] File [{idx} / {len(self.download.downloaded_dates)}] has been created for single-day DHWs.')
        else:
            print(f'[{self.getHrMnSc()}] No new dates have been downloaded. No new DHWs need to be created.')

    def recreateCSVs(self):
        # Get a list of all the files in the nc_sst directory
        files = listdir(self.refined_dir)
        
        # Order files by date
        files = [file[0:10] for file in files]
        files = list(set(files))
        files = sort(files)
        
        # Add .nc to the end of each file
        files = [f'{file}.nc' for file in files]
        
        # Check to see if length of both csv_sst and nc_sst are the same. If so, print a message stating that the files are already converted.
        if len(listdir(f"{self.config['folders']['csv_sst']}")) == len(files):
            print(f'[{datetime.now().strftime("%H:%M:%S")}] All SST files have been converted to csv. No files need to be converted.')
        else:
            # If the length of files is greater than 2500, state this will take a while. Mention the number of files.
            if len(files) > 2500:
                print(f'[{datetime.now().strftime("%H:%M:%S")}] This will take a while. There are {len(files)} files to convert to csv.')
            
            # Load each file and convert to csv, in csv_sst directory.
            # Ensure each has the correct name, as well as lat / lon included in the index and columns.
            for idx, file in enumerate(files):
                # Check to see if the file is already in the csv_sst directory. If so, skip the iteration.
                if path.exists(f"{self.config['folders']['csv_sst']}{file[0:10]}.csv"): continue
                
                # Open the file
                data = nc.Dataset(f"{self.refined_dir}{file}", 'r')
                
                # Get the data
                lons = data.variables['lon'][:]
                lats = data.variables['lat'][:]
                temp = data.variables['analysed_sst'][:]
                
                # Create a DataFrame
                df = DataFrame(temp[0], index=lats, columns=lons)
                
                # Save the DataFrame to a csv file
                df.to_csv(f"{self.config['folders']['csv_sst']}{file[0:10]}.csv")
                
                # Close the file
                data.close()
                
                # If the index is divisible by 1000, print a message stating that the file is being added, out of the total.
                if idx % 1000 == 0:
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] File [{idx} / {len(files)}] was converted to a .csv file.')

if __name__ == '__main__':
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
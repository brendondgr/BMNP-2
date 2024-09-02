## Bonaire Marine National Park
### Set-Up
1. Extract "data" directory from the data .zip file (Labeled "Data - Updated YYYY-MM-DD") and place in main directory.
2. Run setup.sh to initialize the virtual environment for python to run off of (This will set up the all required libraries as well).
3. Activate virtual environment, depending on OS, you will do the following commands:
- Windows: `venv\Scripts\activate`
- Linux: `source venv/bin/activate`
4. To quickly update the database, run:
- `python main.py`

### Troubleshooting
Some troubleshooting may be required.
1. You may need to create an account to download the data remotely. Running `python main.py` from the main directory will prompt you for a Username and Password. This is the same username and password that you need to use to log in here:
- `https://urs.earthdata.nasa.gov/`
2. Sometimes the MUR data does not update for several days, you will likely get an output when running python main.py that looks like the following. Do not worry, it just means that they have not updated it yet.
```
[10:21:53] Downloading the Date: 2024-08-30. Please give this a moment...
[10:21:55] No files were downloaded... Please ensure that the login credentials are correct.
[10:21:55] Downloading the Date: 2024-08-31. Please give this a moment...
[10:21:56] No files were downloaded... Please ensure that the login credentials are correct.
[10:21:56] Downloading the Date: 2024-09-01. Please give this a moment...
[10:21:58] No files were downloaded... Please ensure that the login credentials are correct.
[10:21:58] Creating DHW files...
```

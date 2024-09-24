import matplotlib.pyplot as plt
import numpy as np
from shiny import App, render, ui
from shinywidgets import output_widget, render_widget
import shinyswatch
import netCDF4 as nc
import configparser
from ipyleaflet import Map
import matplotlib.pyplot as plt
import os

# Read config "config.ini" file
config = configparser.ConfigParser()
config.read("config.ini")

# Find the most recent date in the nc_sst folder.
def most_recent_date():
    # List data in folder
    data = os.listdir(config["folders"]["nc_sst"])
    
    # Get the most recent date without the .nc extension
    date = data[-1].replace(".nc", "")
    
    # Print the date
    print(f'Most Recent Date: {date}')
    
    return str(date)

# Create a map UI that 
app_ui = ui.page_fluid(
    ui.page_navbar(
        ui.nav_panel("Daily", ui.page_sidebar(
                                # Sidebar
                                ui.sidebar(
                                # Date Selector
                                ui.input_date("date", "Date", value=most_recent_date()),
                                
                                # Colorbar Types, using input_select
                                ui.input_select("colorbar_type",
                                                "Select a Colorbar Type",
                                                {"RdYlBu_r": "RdYlBu_r", "viridis": "viridis", "plasma": "plasma", "inferno": "inferno", "magma": "magma", "cividis": "cividis"},),
                                
                                ),
                                ui.layout_columns(
                                    # Average Box
                                    ui.value_box(
                                        title = "Average SST",
                                        value = ui.output_ui("calculate_sst"),
                                        theme=str(ui.output_ui("sst_theme")),
                                    ),
                                    ui.value_box(
                                        title = "Average DHW",
                                        # calculate the average DHW from the calculate_dhw function
                                        value = ui.output_ui("calculate_dhw"),
                                        theme=str(ui.output_ui("dhw_theme")),
                                    ),
                                ),
                                ui.layout_columns(
                                    ui.output_plot("plot_sst"),
                                    ui.output_plot("plot_dhw"),
                                ),
                            ),
                        ),
                        
        ui.nav_panel("Monthly", ui.page_fluid(
            
            ),
        ),
        title="Bonaire Dashboard",
        id="page",
        theme = shinyswatch.theme.litera()
    )
)

def server(input, output, session):
    def load_data(vartype = None):
        # Get the date from the input_date widget.
        date = input.date()

        if vartype == "dhw": location = f"{config['folders']['nc_dhw']}"
        elif vartype == "sst": location = f"{config['folders']['nc_sst']}"
        else:
            print("Invalid radio type")
            return

        # Load data in location using netCDF4, as well as the date.nc
        data = nc.Dataset(f'{location}{date}.nc')
        
        # Load the "lat" and "lon" variables from the data
        lat = data.variables['lat'][:]
        lon = data.variables['lon'][:]
        
        # Get the data for the date
        if vartype == "dhw": data = data.variables['dhw']
        elif vartype == "sst": data = data.variables['analysed_sst'][0, :, :] - 273.15
        
        # If the vartype is dhw, np.flip 0 it.
        if vartype == "dhw": data = np.flip(data, 0)
        
        # If the vartype is sst, reverse order of latitude.
        if vartype == "sst": lat = np.flip(lat, 0)
        
        return lat, lon, data, vartype.upper()
    
    # Calculate the average DHW
    @render.text()
    def calculate_dhw():
        # Get date input
        date = input.date()
        
        # Load date from dhw
        data = nc.Dataset(f'{config["folders"]["nc_dhw"]}{date}.nc')
        
        # Get the dhw data from this
        data = data.variables['dhw'][:]
        
        # Calculate the average, from non-nan values
        # Round average to 2 decimal places
        average = round(np.nanmean(data), 2)
        
        return average

    # Calculate the average SST
    @render.text()
    def calculate_sst():
        # Get date input
        date = input.date()
        
        # Load date from dhw
        data = nc.Dataset(f'{config["folders"]["nc_sst"]}{date}.nc')
        
        # Get the dhw data from this
        data = data.variables['analysed_sst'][:]
        
        # Round average to 2 decimal places
        average = round(float(np.nanmean(data))-273.15, 2)
        
        return f"{average}°C"
    
    @render.text()
    def dhw_theme():
        average = calculate_dhw()
        
        if average < 8:
            return "bg-green"
        elif average < 12:
            return "bg-orange"
        else:
            return "bg-red"
    
    @render.text()
    def sst_theme():
        average = calculate_sst()
        
        if average < 27:
            return "bg-green"
        elif average < 30:
            return "bg-orange"
        else:
            return "bg-red"
    
    @render.plot()
    def plot_dhw():
        # Load the data
        lat, lon, data, vartype = load_data(vartype="dhw")
        
        if input.cb_colorbars():
            vmin = 0
            vmax = 30
        else:
            vmin = np.nanmin(data)
            vmax = np.nanmax(data)
        
        fig, ax = plt.subplots()
        cax = ax.imshow(data, cmap='RdYlBu_r', vmin=vmin, vmax=vmax)
        
        if input.cb_colorbars():
            # Minimum 0 and maximum 30.
            plt.colorbar(cax, ticks=[0, 5, 10, 15, 20, 25, 30])
        else:
            plt.colorbar(cax)
        
        # Set the x and y labels
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Set the X and Y Ticks, based on lat and lon
        ax.set_xticks(np.arange(0, len(lon), 5))
        ax.set_xticklabels(lon[::5], rotation=45)
        ax.set_yticks(np.arange(0, len(lat), 5))
        ax.set_yticklabels(lat[::5])
        
        # Add the grid in the background
        ax.grid(True)
        
        # Set the title based on the date and the radio type
        ax.set_title(f"{vartype} for {input.date()} ")
        
        
        return fig

    @render.plot()
    def plot_sst():
        # Load the data
        lat, lon, data, vartype = load_data(vartype="sst")
        
        if input.cb_colorbars():
            vmin = 25
            vmax = 33
        else:
            vmin = np.nanmin(data)
            vmax = np.nanmax(data)
        
        fig, ax = plt.subplots()
        cax = ax.imshow(data, cmap='RdYlBu_r', vmin=vmin, vmax=vmax)
        
        # Check to see first if cb_colorbars is checked
        if input.cb_colorbars():
            # Set the minimum to 25 and maximum to 33
            plt.colorbar(cax, ticks=[25, 26, 27, 28, 29, 30, 31, 32, 33])
        else:
            plt.colorbar(cax)
        
        # Set the x and y labels
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Set the X and Y Ticks, based on lat and lon
        ax.set_xticks(np.arange(0, len(lon), 5))
        ax.set_xticklabels(lon[::5], rotation=45)
        ax.set_yticks(np.arange(0, len(lat), 5))
        ax.set_yticklabels(lat[::5])
        
        # Add the grid in the background
        ax.grid(True)
        
        # Set the title based on the date and the radio type
        ax.set_title(f"{vartype} for {input.date()} ")
        
        return fig

app = App(app_ui, server, debug=True)

# Run the application
app.run(port=8080)
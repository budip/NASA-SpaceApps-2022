from dataclasses import replace
import xarray 
from sunpy.time import parse_time
from sunpy import timeseries as ts 
import pandas as pd 
import urllib
import os
from collections import OrderedDict
from astropy import units as u

DATA_PATH = 'public/data'
BASE_URL = 'https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/'
API_VERSION =  "_v2-1-0"


def get_goes(date, sat=16, path=None):
    """
    Function to download the new GOES 16/17 XRS level 2 science 1s data
    
    Parameters
    ----------
    date : `str`
        date of observation - e.g. '2017-09-10'
    
    sat : `int`, optional
        satellite number, can be 16 or 17. Default is 16
    
    path : `str` 
        path to save file to. Default to cwd.
    
    Returns
    -------
    if file is downloaded or already exists it will return filename/path
    """
   
    date = parse_time(date)
    url = os.path.join(
        BASE_URL, 
        "goes{:d}/l2/data/xrsf-l2-flx1s_science/{:s}/{:s}/sci_xrsf-l2-flx1s_g{:d}_d{:s}{}.nc".format(
            sat, 
            date.strftime('%Y'),
            date.strftime('%m'), 
            sat,
            date.strftime('%Y%m%d'),
            API_VERSION)
        )

    if path is not None:
        filename = os.path.join(path, url.split('/')[-1])
    else:
        filename = url.split('/')[-1]
        
    if os.path.exists(filename):
        return filename
    
    else:
        try:
            urllib.request.urlretrieve(url, filename)

            if os.path.exists(filename):
                return filename
            
        except:
            print("Download failed")
            return None


def make_goes_timeseries(file):
    """
    Function to read in the new GOES 16&17 level2 science data
    and return a sunpy.timeseries
    
    Parameters
    ----------
    file : `str`
        GOES netcdf file 
    
    Returns
    -------
    `sunpy.timeseries.TimeSeries`
    
    """
    
    data = xarray.open_dataset(file)
    
    units = OrderedDict([('xrsa', u.W/u.m**2),
                     ('xrsb', u.W/u.m**2)])
    
    xrsb = data['xrsb_flux']
    xrsa = data['xrsa_flux']

    tt = parse_time(data['time']).datetime
    
    data = pd.DataFrame({'xrsa': xrsa, 'xrsb': xrsb}, index=tt)
    
    data.sort_index(inplace=True)
    
    return ts.TimeSeries(data, units)            


# Input parameters
sat_number  = 16
sensor_type = 'xrs'
date        = '2022-09-28'
output_file = f"{DATA_PATH}/goes-{sat_number}-{sensor_type}-{date.replace('-', '')}.json"



files = get_goes(date=date, sat=16, path=DATA_PATH)

goes_xrs = make_goes_timeseries(files)

goes_16 = ts.TimeSeries(goes_xrs, concat=True)
goes_table = goes_16.to_table()
goes_table.write(output_file, format='pandas.json', overwrite=True)


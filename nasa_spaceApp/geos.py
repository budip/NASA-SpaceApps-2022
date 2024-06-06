import os
import pandas as pd 
import urllib
import urllib.request
import xarray 
from astropy import units as u
from collections import OrderedDict
from sunpy.time import parse_time
from sunpy import timeseries as ts 


APP_NAME            = f'nasa_spaceApp'
DATA_PATH           = f'{APP_NAME}/data'
NGDC_URL            = f'https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/'
SWPC_URL            = f'https://services.swpc.noaa.gov/json/goes/primary'
GOES_ELECTRON       = f'{SWPC_URL}/differential-electrons-7-day.json'
GOES_PROTON         = f'{SWPC_URL}/differential-protons-7-day.json'
GOES_MAGNETOMETERS  = f'{SWPC_URL}/magnetometers-7-day.json'
GOES_XRAYS_FLARES   = f'{SWPC_URL}/xray-flares-7-day.json'
GOES_XRAYS          = f'{SWPC_URL}/xrays-7-day.json'

# TODO: this can be programatically improved in the next iteration
XRS_X1S             = ['xrsf-l2-flx1s_science', 'sci_xrsf-l2-flx1s_g', '_v2-1-0']
XRS_SUM             = ['xrsf-l2-flsum_science', 'sci_xrsf-l2-flsum_g', '_v2-1-0']
XRS_LOC             = ['xrsf-l2-flloc_science', 'sci_xrsf-l2-flloc_g', '_v2-1-0']
XRS_DET             = ['xrsf-l2-fldet_science', 'sci_xrsf-l2-fldet_g', '_v2-1-0']
XRS_KD1             = ['xrsf-l2-bkd1d_science', 'sci_xrsf-l2-bkd1d_g', '_v2-1-0']
EPHE                = ['ephe-l2-orb1m', 'dn_ephe-l2-orb1m_g', '_v0-0-3']
EUV_AVG1D           = ['euvs-l2-avg1d_science', 'sci_euvs-l2-avg1d_g', '_v1-0-1']
EUV_AVG1M           = ['euvs-l2-avg1m_science', 'sci_euvs-l2-avg1m_g', '_v1-0-1']
MAG_HI              = ['magn-l2-hires', 'dn_magn-l2-hires_g', '_v1-0-1']


def construct_path(type):
    """
    Function to construct url path to a specific instrumentation data.
    
    Parameters
    ----------
    type : `str`
        Instrumentation type.
    
    Returns
    -------
    Path one, Path two and Version.

    """    
    if type == 'xrs-x1s': 
        path_one = XRS_X1S[0]
        path_two = XRS_X1S[1]
        version = XRS_X1S[2]
    elif type == 'xrs-sum':
        path_one = XRS_SUM[0]
        path_two = XRS_SUM[1]
        version = XRS_SUM[2]
    elif type == 'xrs-loc':
        path_one = XRS_LOC[0]
        path_two = XRS_LOC[1]
        version = XRS_LOC[2]      
    elif type == 'xrs-det':
        path_one = XRS_DET[0]
        path_two = XRS_DET[1]
        version = XRS_DET[2]
    elif type == 'xrs-kd1':
        path_one = XRS_KD1[0]
        path_two = XRS_KD1[1]
        version = XRS_KD1[2]
    elif type == 'ephe':
        path_one = EPHE[0]
        path_two = EPHE[1]
        version = EPHE[2]
    elif type == 'euv-avg1d':
        path_one = EUV_AVG1D[0]
        path_two = EUV_AVG1D[1]
        version = EUV_AVG1D[2]
    elif type == 'euv-avg1m':
        path_one = EUV_AVG1M[0]
        path_two = EUV_AVG1M[1]
        version = EUV_AVG1M[2]       
    elif type == 'mag-hi':
        path_one = MAG_HI[0]
        path_two = MAG_HI[1]
        version = MAG_HI[2]
    else:
        path_one = XRS_X1S[0]
        path_two = XRS_X1S[1]
        version = XRS_X1S[2]

    return path_one, path_two, version


def get_goes(date, sat=16, type='xrs', path=None):
    """
    Function to download the new GOES 16/17/18 XRS level 2 science 1s data
    
    Parameters
    ----------
    date : `str`
        date of observation - e.g. '2017-09-10'
    
    type : `str`
        satellite type. Allowed values: 'ephe', 'xrs-x1s', 'xrs-sum', 'xrs-loc', 'xrs-det', 'xrs-kd1', 'euv-avg1d', 'euv-avg1m', 'mag-hi'

    sat : `int`, optional
        satellite number, can be 16 or 17 or 18. Default is 16
    
    path : `str` 
        path to save file to. Default to cwd.
    
    Returns
    -------
    if file is downloaded or already exists it will return filename/path

    """

    path_one, path_two, version = construct_path(type)
    date = parse_time(date)
    url = os.path.join(
        NGDC_URL, 
        "goes{:d}/l2/data/{}/{:s}/{:s}/{}{:d}_d{:s}{}.nc".format(
                sat, 
                path_one,
                date.strftime('%Y'),
                date.strftime('%m'), 
                path_two,
                sat,
                date.strftime('%Y%m%d'),
                version
            )
        )
    print (url)

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
    
    units = OrderedDict([('xrsa', u.W/u.m**2), ('xrsb', u.W/u.m**2)])
    
    xrsb = data['xrsb_flux']
    xrsa = data['xrsa_flux']

    tt = parse_time(data['time']).datetime
    
    data = pd.DataFrame({'xrsa': xrsa, 'xrsb': xrsb}, index=tt)
    data.sort_index(inplace=True)
    
    return ts.TimeSeries(data, units)            


def read_recent_noaa(interval="7-day"):
    """
    Function to read the NOAA recent GOES-16 XRS data and return a 
    sunpy.timeseries
    
    Parameters
    ----------
    interval : `str`, optional - default past 7 days
        past interval file to read.
        
    Returns
    -------
    sunpy.timeseries
    
    """
    dict_interval = {
        "7-day": "xrays-7-day.json", 
        "3-day": "xrays-3-day.json", 
        "1-day": "xrays-1-day.json",
        "6-hour": "xrays-6-hour.json"
    }
    
    if interval not in dict_interval.keys():
        print("interval must be one of", dict_interval.keys())
    
    noaa_file = "{}/{:s}".format(SWPC_URL, dict_interval[interval])
    units = OrderedDict([('xrsa', u.W/u.m**2), ('xrsb', u.W/u.m**2)])

    data = pd.read_json(noaa_file)

    data_short = data[data['energy']=='0.05-0.4nm']
    data_long = data[data['energy']=='0.1-0.8nm']
    time_array = [parse_time(x).datetime for x in data_short['time_tag'].values]

    df = pd.DataFrame({'xrsa': data_short['flux'].values, 'xrsb': data_long['flux'].values}, index=time_array)
    df.sort_index(inplace=True)
    
    return ts.TimeSeries(df, units)


def get_goes_by_date():
    """
    Function to execute goes data by date.
    
    Parameters
    ----------
    None
        
    Returns
    -------
    None. Files will be created.
    
    """
    satList  = [16, 17, 18]     #[16, 17, 18]
    typeList = ['xrs-x1s']      #['ephe', 'xrs-x1s', 'xrs-sum', 'xrs-loc', 'xrs-det', 'xrs-kd1', 'euv-avg1d', 'euv-avg1m', 'mag-hi']
    dateList = ['2022-10-02', '2022-10-01', '2022-09-30', '2022-09-29', '2022-09-28', '2022-09-27', '2022-09-26', '2022-09-25', '2022-09-24', '2022-09-23', '2022-09-22', '2022-09-21', '2022-09-20']

    for type in typeList:
        for sat in satList:
            for date in dateList:
                try:
                    output_file = f"{DATA_PATH}/goes-{sat}-{type}-{date.replace('-', '')}.json"

                    files = get_goes(date=date, sat=sat, type=type, path=DATA_PATH)

                    goes_xrs = make_goes_timeseries(files)
                    goes_16 = ts.TimeSeries(goes_xrs, concat=True)
                    
                    goes_table = goes_16.to_table()
                    goes_table.write(output_file, format='pandas.json', overwrite=True)

                    print (f'  ==> {output_file}')
                except:
                    continue


def get_goes_last_7d_deprecated():
    """
    Function to execute goes last 7 days data.
    
    Parameters
    ----------
    None
        
    Returns
    -------
    None. Files will be created.
    
    """
    output_file = f"{DATA_PATH}/result.json"

    goes_xrs_7days = read_recent_noaa()
    goes = ts.TimeSeries(goes_xrs_7days, concat=True)

    goes_table = goes.to_table()
    goes_table.write(output_file, format='pandas.json', overwrite=True)


def get_goes_last_7d_complete():
    """
    Function to get primary measurements from goes last 7 days data.
    
    Parameters
    ----------
    None
        
    Returns
    -------
    None. Files will be created under data/7-day directory.
    
    """

    sensor_type = {
        GOES_ELECTRON: 'electron.json',
        GOES_PROTON: 'proton.json',
        GOES_MAGNETOMETERS: 'magnetometers.json',
        GOES_XRAYS_FLARES: 'xrays-flares.json',
        GOES_XRAYS: 'xrays.json'
    }
    result = ''

    for key in sensor_type:
        with urllib.request.urlopen(key) as url:
            result = url.read()
            print (f'{url.url}')

        output_file = f'{DATA_PATH}/7-day/{sensor_type[key]}'
        with open(output_file, 'w') as f:
            f.writelines(result.decode('utf-8'))
            print (f'  ==> {output_file}')
            

# Comment these lines (one or both) before executing the program.
# get_goes_by_date()
# get_goes_last_7d_complete()

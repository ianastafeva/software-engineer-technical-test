import datetime
import urllib
import os
import pandas as pd
import asyncio
import aiohttp
import io


def build_api_url(latitude,longitude,radius,minimum_magnitude,end_date):
    
    """
    Build url to retrieve a catalogue of earthquakes that took place in a give region from 200 year 
    before a given date from USGS database
    
    Parameters
    ----------
    latitude: latitude of region central point in degrees (float or int)
    longitude: longitude of region central point in degrees (float or int)
    radius: the maximum radius from the central point defining the retrieval region in km (float or int)
    minimum_magnitude: the minimum magnitude of earthquake below which data are not retrieved (float or int)
    end_date: the date where data from 200 year before are retrieved (datetime.datetime)
    
    Returns
    --------
    a string containing the requested url
    
    Example
    --------
    ret_url = get_earthquake_data(-20,-175,1000,3,datetime(year=2021, month=10, day=21))
    
    """
                  
                  
    st_date = end_date - datetime.timedelta(days=(200*365.24))
    st_date = st_date.strftime("%Y-%m-%d")

    end_date = end_date.strftime("%Y-%m-%d")

    minimum_magnitude = str(minimum_magnitude)
    radius = str(radius)
    latitude = str(latitude)
    longitude = str(longitude)

    usgs_page = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv'

    url = usgs_page + '&starttime=' + st_date + '&endtime=' + end_date + '&minmagnitude=' + minimum_magnitude + '&latitude=' + latitude + '&longitude=' + longitude + '&maxradiuskm=' + radius
                
                
    return url
                  

def get_earthquake_data(latitude=-20,longitude=-175,
                        radius=1000,minimum_magnitude=5,
                        end_date=datetime.datetime(year=2021, month=10, day=21)):
    
    """
    Retrieve a catalogue of earthquakes that took place in a given region from 200 year 
    before a given date from USGS database
    
    Parameters
    ----------
    latitude: latitude of region central point in degrees (float or int)
    longitude: longitude of region central point in degrees (float or int)
    radius: the maximum radius from the central point defining the retrieval region in km (float or int)
    minimum_magnitude: the minimum magnitude of earthquake below which data are not retrieved (float or int)
    end_date: the date where data from 200 year before are retrieved (datetime.datetime)
    
    Returns
    --------
    if the code works: catalogue of earthquakes in pd.dataframe format
    if the code fails: the retrieval url to be run through a web browser to investigate the error
    
    Example
    --------
    
    Eq_catalogue = get_earthquake_data(latitude=-20,longitude=-175,
                   radius=1000,minimum_magnitude=3,end_date=datetime.datetime(year=2021, month=10, day=21))
    
    
    
    
    """
    

    download_url = build_api_url(latitude,longitude,radius,minimum_magnitude,end_date)
    
    end_date = end_date.strftime("%Y-%m-%d")

    minimum_magnitude = str(minimum_magnitude)
    radius = str(radius)
    latitude = str(latitude)
    longitude = str(longitude)
    
    cwd = os.getcwd()
    cwd = cwd + '/'
    cat_name = 'Earthquake_cat_mMw'+minimum_magnitude+'_Clat'+latitude+'d_clon'+longitude+'d_Mrad'+radius+'km_EndDate'+end_date+'.csv'
    
    try:
        urllib.request.urlretrieve(download_url, cwd + cat_name)
        cat = pd.read_csv(cwd + cat_name)
        print('worked')
        return cat
        
    except:
        print('did not work')
        return download_url, cat_name
    

       
async def fetch_file(url, session):
        async with session.get(url) as resp:
            assert resp.status == 200
            catalogue = await resp.read()
            return catalogue.decode()

        
async def get_earthquake_data_for_multiple_locations(assets, radius=200, minimum_magnitude=4.5, 
    end_date=datetime.datetime(year=2021, month=10, day=21)):

    """
    Retrieve a catalogue of earthquakes that took place in a given list of region from 200 year 
    before a given date from USGS database
    
    Parameters
    ----------
    assest: 2d numpy array contain the latitudes and longitudes that define the centers of the regions in questions
    radius: the maximum radius from the central point defining the retrieval region in km (float or int)
    minimum_magnitude: the minimum magnitude of earthquake below which data are not retrieved (float or int)
    end_date: the date where data from 200 year before are retrieved (datetime.datetime)
    
    Returns
    --------
    if the code works: catalogue of earthquakes in pd.dataframe format
    if the code fails: None
    
    """
    
    cwd = os.getcwd()
    cwd = cwd + '/'
    async with aiohttp.ClientSession() as session:
        list_requests = []
        for latitude, longitude in assets.tolist():
            download_url = build_api_url(latitude,longitude,radius,minimum_magnitude,end_date)

            list_requests.append(asyncio.ensure_future(fetch_file(download_url, session)))
                                
        list_catalogues = await asyncio.gather(*list_requests)
                                 
        Eq_catalogues = None
        for catalogue in list_catalogues:
            catalogue = pd.read_csv(io.StringIO(catalogue))
            
            if Eq_catalogues is None:
               Eq_catalogues = catalogue
            else:
                Eq_catalogues = pd.concat([Eq_catalogues, catalogue])
                Eq_catalogues = Eq_catalogues.drop_duplicates()
                Eq_catalogues = Eq_catalogues.reset_index(drop = True)
                        
        
    
    return Eq_catalogues
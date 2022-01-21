EARTH_RADIUS = 6378

TIME_COLUMN = "time"
PAYOUT_COLUMN = "payout"
MAGNITUDE_COLUMN = "mag"
DISTANCE_COLUMN = "distance"
LATITUDE_COLUMN = "latitude"
LONGITUDE_COLUMN = "longitude"

import numpy as np

def get_haversine_distance(lats, lons, lat0, lon0):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    
    Parameters
    ----------
    lats: latitudes of points that their distance from a give point are computed
    lons: longitudes of points that their distance from a give point are computed
    lat0: latitude of the given point from which the distance is computed 
    lon0: longitude of the given point from which the distance is computed
    
    Return:
    -------
    The great circle distance between the given points
    """
    
    # convert decimal degrees to radians
    lats = lats * (np.pi/180)
    lons = lons * (np.pi/180)
    lat0 = lat0 * (np.pi/180)
    lon0 = lon0 * (np.pi/180)

    # haversine formula 
    dlon = lons - lon0 
    dlat = lats - lat0
    a = np.sin(dlat/2)**2 + np.cos(lat0) * np.cos(lats) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    r = EARTH_RADIUS # Radius of earth in kilometers
    return c * r


def compute_payouts(earthquake_data, payouts_str):
    
    """
    To compute the amount of payout for each given year in the payout dictionary
    
    Parameters:
    -----------
    earthquake_data: pd.dataframe containing the earthquakes catalogue
    payout_str: pd.dataframe containing the radius (distance) and magintude and thier corresponding payout
    
    Returns:
    --------
    Python dictionary containing the given years and their amount of payouts
    
    
    """
    
    payout_years = {}
    
    for t, mg , dist in earthquake_data[[TIME_COLUMN, MAGNITUDE_COLUMN, DISTANCE_COLUMN]].values:
        year = int(t[0:4])
        
        payout = 0
        for dist_str, mg_str, payout_str in payouts_str[['Radius', 'Magnitude', 'Payout']].values:
            if (float(dist) <= float(dist_str)) and (float(mg) >= float(mg_str)):
                if payout < payout_str:
                    payout = payout_str
                    
        if year in payout_years:
            if payout_years[year] < payout:
                payout_years[year] = payout
          
        else:
            payout_years[year]  = payout
        
        
    return payout_years
                
    
def compute_burning_cost(payouts, start_year=1952, end_year=2021):
    
    """
    To compute the burning cost (the average of payouts over a time range) given a payout dictionary
    
    Parameters:
    -----------
    payouts: python dictionary containing the given years and their amount of payouts
    start_year: the start of the time range in years
    end_year: the end of the time range in years
    
    Returns:
    --------
    the burning cost over the given time range
    
    
    """
    
    payout_years = list(payouts.keys())
    
    burn = []
    for year in payout_years:
        if (year >= start_year) and (year <= end_year):
            burn.append(payouts[year])
    
    if len(burn) < 1:
        print('the time limits entered does not correspond to any data in the payout dictionary')
        return None
    else:
        burning_cost = np.asarray(burn).sum() /  (end_year - start_year + 1)
        return burning_cost
    
                
        
        
        
    
    

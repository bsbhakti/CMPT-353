from gettext import npgettext
import sys
import pandas as pd
import numpy as np
import pathlib
import matplotlib.pyplot as plt
from math import radians, cos, sin, asin, sqrt


def distance(stations,cities):
            cities[['lat_rad', 'lon_rad']] = np.deg2rad(cities[['latitude', 'longitude']])
            stations[['lat_rad', 'lon_rad']] = np.deg2rad(stations[['latitude', 'longitude']])

            city_lats = cities['lat_rad'].values[:, np.newaxis]
            city_lons = cities['lon_rad'].values[:, np.newaxis]
            station_lats = stations['lat_rad'].values
            station_lons = stations['lon_rad'].values

            dlat = city_lats - station_lats 
            dlon = city_lons - station_lons 
            a = np.sin(dlat/2)**2 + np.cos(city_lats) * np.cos(station_lats) * np.sin(dlon / 2)**2
            c = 2 * np.arcsin(np.sqrt(a))
            r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
            d2 = c * r * 1000 # in meters
    
            df = pd.DataFrame(d2, columns=stations['station'], index=cities['name'])
            #print(df)
            return df

def best_tmax(cityRow,stations):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """

    stations = stations.set_index('station')
    min = cityRow.idxmin()
    return stations.loc[min]["avg_tmax"]


stations_file = pathlib.Path(sys.argv[1])
city_file = pathlib.Path(sys.argv[2])
output_file = pathlib.Path(sys.argv[3])

stations = pd.read_json(stations_file, lines=True)
city = pd.read_csv(city_file,dtype={'longitude':np.float64, 'latitude':np.float64})
stations['avg_tmax'] = stations['avg_tmax'] /10
city = city[city['population'].notna()]
city = city[city['area'].notna()]
city['area'] = city['area']/ 1e+6
city['population_density'] = city['population']/city['area']
city = city[city['area'] <= 10000]
city.reset_index(drop=True,inplace=True)

res = distance(stations,city)
res.reset_index(drop=True,inplace=True)

city["temp"] = (res.apply(best_tmax,axis=1,stations=stations))
print(city)
plt.plot(city['temp'],city['population_density'],'b.')
plt.xlabel('Avg Max Temperature (\u00b0C)')
plt.ylabel('Population Density (people/km\u00b2)')
plt.savefig(output_file)


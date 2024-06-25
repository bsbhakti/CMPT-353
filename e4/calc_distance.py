import sys
from xml.dom.minidom import parse, parseString
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from pykalman import KalmanFilter
import matplotlib.pyplot as plt

def diff(row,count):
    lon1, lat1 = (float(row['longitiude'])), (float(row['latitude']))
    lon2, lat2 = (float(row['longitiude_1'])), (float(row['latitude_1']))
    d1 = abs(lon1 -lon2)
    d2 = abs(lat1 - lat2)
    if(d1 >=0.00001 or d2 >=0.00001):
        count+=1
        # print("true")


# found this method here: https://stackoverflow.com/questions/4913349/distance-formula-in-python-bearing-and-distance-between-two-gps-points
def distance(row):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians

    lon1, lat1 = radians(float(row['longitiude'])), radians(float(row['latitude']))
    lon2, lat2 = radians(float(row['longitiude_1'])), radians(float(row['latitude_1']))


    # distance formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r * 1000 # in meters

def get_data(input_gpx):
    tree = ET.parse(f"./{input_gpx}")
    root = tree.getroot()
    d = []
    # df = pd.DataFrame()
    for x in root[0][0]:
        # print(x.attrib['lat'])
        # print(x[0].text)
        d.append({
            'longitiude': x.attrib['lon'],
            'latitude': x.attrib['lat'],
            'datetime': x[0].text,

            
        })
    df = pd.DataFrame(d)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    return df

       
        

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['latitude']))
        trkpt.setAttribute('lon', '%.7f' % (pt['longitiude']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def main():
    input_gpx = sys.argv[1]
    input_csv = sys.argv[2]
    
    points = get_data(input_gpx).set_index('datetime')

    sensor_data = pd.read_csv(input_csv, parse_dates=['datetime']).set_index('datetime')
    points['Bx'] = sensor_data['Bx']
    points['By'] = sensor_data['By']

    points['latitude_1'] = points['latitude'].shift(periods=[1])['latitude_1'].fillna(0).astype(float)
    points['longitiude_1'] = points['longitiude'].shift(periods=[1])['longitiude_1'].fillna(0).astype(float)
    pd.set_option('display.float_format', '{:.8f}'.format)
    
    # print(points)
    dist = []

    # points1 = pd.DataFrame({
    # 'latitude': [49.28, 49.26, 49.26],
    # 'longitiude': [123.00, 123.10, 123.05]})
    # # print(f"this is before \n{points1}\n")

    # points1['latitude_1'] = points1['latitude'].shift(periods=[1])['latitude_1'].fillna(0).astype(float)
    # points1['longitiude_1'] = points1['longitiude'].shift(periods=[1])['longitiude_1'].fillna(0).astype(float)
    # print(f"this is after \n{points1}\n")

    dist = points.iloc[1:,:].apply(distance,axis=1)
    # print((dist))
    print(f'Unfiltered distance: {dist.sum():.2f}')
    points = points.drop(["latitude","longitiude"],axis=1)
    points = points.iloc[1:]
    points = points[['Bx', 'By', 'latitude_1', 'longitiude_1']]
    # print(points)

    initial_state = points.iloc[1]
    observation_covariance = np.diag([5,5, 5/1e5, 5/1e5]) **2
    transition_covariance = np.diag([7, 7, 3/1e5, 3/1e5]) **2
    # transition_covariance = np.diag([7, 7, 0.00000005, 0.00000005]) **2

    # print(points)
    transition = [[1,0,0,0],[0,1,0,0],[5*10**(-7),34*10**(-7),1,0],[-49*10**(-7),9*10**(-7),0,1]]

    kf = KalmanFilter(   initial_state_mean=initial_state,
                    initial_state_covariance=observation_covariance,
                    observation_covariance=observation_covariance,
                    transition_covariance=transition_covariance,
                        transition_matrices=transition )

    kalman_smoothed, _ = kf.smooth(points)
    dataset = pd.DataFrame({'Bx': kalman_smoothed[:, 0], 'By': kalman_smoothed[:, 1],'latitude': kalman_smoothed[:, 2],'longitiude': kalman_smoothed[:, 3]})
    dataset['latitude_1'] = dataset['latitude'].shift(periods=[1])['latitude_1'].fillna(0).astype(float)
    dataset['longitiude_1'] = dataset['longitiude'].shift(periods=[1])['longitiude_1'].fillna(0).astype(float)
    smoothed_dist = dataset.iloc[1:,:].apply(distance,axis=1)
    print(f'Filtered distance: {smoothed_dist.sum():.2f}')

    dataset = dataset.drop(["latitude_1","longitiude_1"],axis =1)
    dataset = dataset.iloc[1:]

    points = points.rename(columns={'latitude_1': 'latitude', 'longitiude_1': 'longitiude'})

    output_gpx(dataset, 'out.gpx')
    output_gpx(points, 'outobs.gpx')




if __name__ == '__main__':
    main()

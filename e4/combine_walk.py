import os
import pathlib
import sys
from threading import local
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'
    
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['latitude']))
        trkpt.setAttribute('lon', '%.10f' % (pt['longitiude']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['timestamp'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))


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
            'datetime': x[1].text,
            'ele': x[0].text,
        })
    df = pd.DataFrame(d)
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True,yearfirst=True,format='mixed')
    return df


def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])
    
    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    first_time = accl['timestamp'].min()
    offset = 0
    gps['timestamp'] = gps['datetime'].dt.round('4s')
    accl['timestamp'] = accl['timestamp'].dt.round('4s')

    gps[['latitude', 'longitiude', 'ele']] = gps[['latitude', 'longitiude', 'ele']].astype(float)
    gps = gps.groupby(['timestamp']).mean()
    accl = accl.groupby(['timestamp']).mean()


    # TODO: create "combined" as described in the exercise
    corr = 0
    best_offset = 0
    for offset in np.linspace(-5.0, 5.0, 101):
        phone['timestamp'] = first_time + pd.to_timedelta(phone['time'] + offset, unit='sec')
        phone['timestamp'] = phone['timestamp'].dt.round('4s')
        phone.reset_index(drop=True,inplace=True)
        phone_new = phone.groupby(['timestamp']).mean()

    
    # Calculate correlation
        local_corr = (phone_new['gFx'] * accl['x']).sum()

        if( corr < local_corr):
            corr = local_corr
            best_offset = offset

    print(f'Best time offset: {best_offset:.1f}')

    phone['timestamp'] = first_time + pd.to_timedelta(phone['time'] + best_offset, unit='sec')
    phone['timestamp'] = phone['timestamp'].dt.round('4s')

    phone.reset_index(drop=True,inplace=True)
    phone_new = phone.groupby(['timestamp']).mean()
    phone.reset_index(inplace=True)
    phone_new.reset_index(inplace=True)

    # print(phone_new)
    # print(gps)


    
    os.makedirs(output_directory, exist_ok=True)
    combined = pd.merge_asof(
    phone_new,
    gps,
    left_on='timestamp',
    right_on='timestamp',
    direction='nearest'
)

# Select only the necessary columns for the combined DataFrame
    combined = combined[['timestamp', 'latitude', 'longitiude', 'Bx', 'By']]

    combined = combined.dropna()
    # print(combined)

    output_gpx(combined[['timestamp', 'latitude', 'longitiude']], output_directory / 'walk.gpx')
    combined[['timestamp', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)


main()


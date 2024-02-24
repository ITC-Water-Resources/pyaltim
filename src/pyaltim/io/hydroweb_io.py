import os
import pandas as pd
import geopandas as gpd
import io
from datetime import datetime
from shapely import Point, MultiPoint
import numpy as np

def decyear2dt(decyear):
    """Convert a decimal year to a datetime object"""
    decyear=float(decyear)
    year=int(decyear)
    jan1=datetime(year,1,1)
    return np.datetime64(jan1+(decyear-year)*(datetime(year+1,1,1)-jan1),'ms')

def readHydroWeb_Lakes(file_obj):

    if type(file_obj) == str:
        if not os.path.exists(file_obj):
            raise OSError(f"{file_obj} not found")
        fid=open(file_obj,'rt')
    else:
        fid=file_obj

   # read first line and 
    line=fid.readline().split(";")
    hwbdict={}
    lonlat={}
    for entry in line:
        ky,val=entry.split("=")
        if ky in ["date","last_date","first_date"]:
            val=decyear2dt(float(val))
        elif ky in ["lon","lat"]:
            lonlat[ky]=float(val)
            continue
        elif ky == "id":
            #rename
            ky="hydrowebid"
        hwbdict[ky]=[val]
    

    hwbdict['readme']=""
    hwbdata={}
    for ky in ["time","water_level","water_level_std","area","volume"]:
        hwbdict[ky]=None
        hwbdata[ky]=[]
    

    # now also read the remaining data and  header info
    for line in fid:

        if line.startswith("#"):
            hwbdict['readme']+=line
        else:
            #data
            lnspl=line.split(";")
            hwbdata['time'].append(decyear2dt(lnspl[0]))
            hwbdata['water_level'].append(float(lnspl[3]))
            hwbdata['water_level_std'].append(float(lnspl[4]))
            hwbdata['area'].append(float(lnspl[5]))
            hwbdata['volume'].append(float(lnspl[6]))
    
    geometry=Point(lonlat['lon'],lonlat['lat'])
    
    #construct a geopandas pandas dataframe
    geometry=[Point(lonlat['lon'],lonlat['lat'])]
    df=gpd.GeoDataFrame(hwbdict,crs="EPSG:4326",geometry=geometry)
    df=df.rename(columns={'date':'lastmod','last_date':'tend','first_date':'tstart'}) 
    
    #also assign data
    for ky,val in hwbdata.items():
        df.at[0,ky]=np.asarray(val)

    if type(file_obj) == str:
        # close if it was opened in this routine
        fid.close()

    return df

def readHydroWeb_Rivers(file_obj):

    if type(file_obj) == str:
        if not os.path.exists(file_obj):
            raise OSError(f"{file_obj} not found")
        fid=open(file_obj,'rt')
    else:
        fid=file_obj
    hwbdict={}
    

    headermap={"#BASIN":"basin","#RIVER":"river","#ID":"hydrowebid","#MISSION(S)-TRACK(S)":"missions","#MEAN ALTITUDE":"mean_alt","#FIRST DATE IN DATASET":"tstart","#LAST DATE IN DATASET":"tend","#PRODUCTION DATE":"lastmod","#REFERENCE LONGITUDE":"reflon","#REFERENCE LATITUDE":"reflat","#PRODUCT VERSION":"version","#PRODUCT CITATION":"citation"}

    # read in the header info
    for line in fid:
        if line.startswith("###"):
            #end of header part
            break

        lnspl=line.split("::")
        if lnspl[0] in headermap:
            ky=headermap[lnspl[0]]
            val=lnspl[1].strip()
            hwbdict[ky]=[val]
    refpoint=Point(float(hwbdict['reflon'][0]),float(hwbdict['reflat'][0]))

    datamap={"water_level":2,"water_level_std":3,"mission":10,"groundtrack":12,"cycle":13,"retrack":14}
    fill=9999.999
    hwbdata={"time":[]}
    hwbdict["time"]=None
    for ky in datamap.keys():
        hwbdict[ky]=None
        hwbdata[ky]=[]
    pnts=[]
    #loop over the remaining data
    for line in fid:
        lnspl=line.split()
        # retrieve the time stamp
        hwbdata["time"].append(np.datetime64(line[0:16]))
        #get lon,lat point
        lon=float(lnspl[5])
        lat=float(lnspl[6])
        if lon == fill or lat == fill:
            pnts.append(refpoint)
        else:
            pnts.append(Point(lon,lat))

        for ky,col in datamap.items():
            val=lnspl[col]
            hwbdata[ky].append(val)

    df=gpd.GeoDataFrame(hwbdict,crs="EPSG:4326",geometry=[MultiPoint(pnts)])
   
    #also assign data
    for ky,val in hwbdata.items():
        df.at[0,ky]=np.asarray(val)


    if type(file_obj) == str:
        # close if it was opened in this routine
        fid.close()
    return df

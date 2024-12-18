import os
import pandas as pd
import geopandas as gpd
import io
from datetime import datetime
from shapely import Point, MultiPoint
import numpy as np
from pystac_client import Client
from pystac_client.exceptions import APIError
from pyaltim.core.logging import altlogger
import json
import shapely
import requests
import xarray as xr
from pyaltim.portals.api import APILimitReached
import getpass

def decyear2dt(decyear):
    """Convert a decimal year to a datetime object"""
    decyear=float(decyear)
    year=int(decyear)
    jan1=datetime(year,1,1)
    return jan1+(decyear-year)*(datetime(year+1,1,1)-jan1)

def decyear2iso(decyear):
    """Convert a decimal year to a iso datestamp"""
    decyear=float(decyear)
    year=int(decyear)
    jan1=datetime(year,1,1)
    return (jan1+(decyear-year)*(datetime(year+1,1,1)-jan1)).isoformat()

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
    kymap={"date":"lastupdate","id":"hydrowebid","last_date":"tend","first_date":"tstart"}
    for entry in line:
        ky,val=entry.split("=")
        if ky in ["date","last_date","first_date"]:
            val=decyear2dt(float(val))
        elif ky in ["lon","lat"]:
            lonlat[ky]=float(val)
            continue
        if ky in kymap:
            #possibly rename keys
            ky=kymap[ky]
        hwbdict[ky]=val
    
    hwbdict['readme']=""
    hwbdata={}
    for ky in ["time","water_level","water_level_std","area","volume"]:
        # hwbdict[ky]=None
        hwbdata[ky]=[]
    

    # now also read the remaining data and  header info
    for line in fid:

        if line.startswith("#"):
            hwbdict['readme']+=line
        else:
            #data
            lnspl=line.split(";")
            hwbdata['time'].append(decyear2iso(lnspl[0]))
            hwbdata['water_level'].append(float(lnspl[3]))
            hwbdata['water_level_std'].append(float(lnspl[4]))
            hwbdata['area'].append(float(lnspl[5]))
            hwbdata['volume'].append(float(lnspl[6]))
    
    

        # geometry=point(lonlat['lon'],lonlat['lat'])
        # #construct a geopandas pandas dataframe
        # dout=gpd.geodataframe(hwbdict,crs="epsg:4326",geometry=[geometry])
        # dout=dout.rename(columns={'date':'lastupdate','last_date':'tend','first_date':'tstart'}) 
    
        # #also assign data
        # for ky,val in hwbdata.items():
            # dout.at[0,ky]=np.asarray(val)
    dout=xr.Dataset({ky:("time",val) for ky,val in hwbdata.items()},attrs={ky:val for ky,val in hwbdict.items() if ky not in ["tstart","tend","lastupdate"]})
    dout['lon']=lonlat['lon']
    dout['lat']=lonlat['lat']
    if type(file_obj) == str:
        # close if it was opened in this routine
        fid.close()

    return hwbdict,dout

def readHydroWeb_Rivers(file_obj):

    if type(file_obj) == str:
        if not os.path.exists(file_obj):
            raise OSError(f"{file_obj} not found")
        fid=open(file_obj,'rt')
    else:
        fid=file_obj
    hwbdict={}
    

    headermap={"#BASIN":"basin","#RIVER":"river","#ID":"hydrowebid","#MISSION(S)-TRACK(S)":"missions","#MEAN ALTITUDE":"mean_alt","#FIRST DATE IN DATASET":"tstart","#LAST DATE IN DATASET":"tend","#PRODUCTION DATE":"lastupdate","#REFERENCE LONGITUDE":"reflon","#REFERENCE LATITUDE":"reflat","#PRODUCT VERSION":"version","#PRODUCT CITATION":"citation"}

    # read in the header info
    for line in fid:
        if line.startswith("###"):
            #end of header part
            break

        lnspl=line.split("::")
        if lnspl[0] in headermap:
            ky=headermap[lnspl[0]]

            val=lnspl[1].strip()
            hwbdict[ky]=val
    refpoint=Point(float(hwbdict['reflon']),float(hwbdict['reflat']))

    datamap={"water_level":2,"water_level_std":3,"mission":10,"groundtrack":12,"cycle":13,"retrack":14,"lon":5,"lat":6}
    fill=9999.999
    hwbdata={"time":[]}
    for ky in datamap.keys():
        # hwbdict[ky]=None
        hwbdata[ky]=[]
    pnts=[]
    #loop over the remaining data
    for line in fid:
        lnspl=line.split()
        # retrieve the time stamp
        # hwbdata["time"].append(np.datetime64(line[0:16]))
        hwbdata["time"].append(line[0:16])
        #get lon,lat point
        # lon=float(lnspl[5])
        # lat=float(lnspl[6])
        # if lon == fill or lat == fill:
            # pnts.append(refpoint)
        # else:
            # pnts.append(Point(lon,lat))

        for ky,col in datamap.items():
            val=lnspl[col]
            if ky not in ["retrack","mission","time"]:
                val=float(val)
            hwbdata[ky].append(val)
    
    dout=xr.Dataset({ky:("time",val) for ky,val in hwbdata.items()},attrs={ky:val for ky,val in hwbdict.items() if ky not in ["tstart","tend","lastupdate"]})
    

    # df=gpd.GeoDataFrame(hwbdict,crs="EPSG:4326",geometry=[MultiPoint(pnts)])
   
    #also assign data
    # for ky,val in hwbdata.items():
        # df.at[0,ky]=np.asarray(val)


    if type(file_obj) == str:
        # close if it was opened in this routine
        fid.close()
    return hwbdict,dout


class HydrowebConnect:
    products=["HYDROWEB_RIVERS_RESEARCH","HYDROWEB_RIVERS_OPE","HYDROWEB_LAKES_RESEARCH","HYDROWEB_LAKES_OPE"]
    def __init__(self,collection_id,apikey=None):
        if apikey is None:
            apikey=getpass.getpass("Please enter apikey for hydroweb next (theia)")
        if collection_id not in self.products:
            raise RuntimeError(f"Collection_id must be one of {self.products}")
        self.collection_id=collection_id
        self._collection=None
        self._client=None
        self.headers={"X-API-Key":apikey,"Accept": "application/json","Content-Type": "application/json","User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0"}
        
        #assign the appropriate assets data reader
        if "LAKES" in collection_id:

            self.readasset=readHydroWeb_Lakes
        else:
            self.readasset=readHydroWeb_Rivers
        self.apicalls=0

    @property
    def client(self):
        if self._client is None:
            self._client=Client.open("https://hydroweb.next.theia-land.fr/api/v1/rs-catalog/stac",headers=self.headers)
            self.apicalls+=1
        
        return self._client

    @property
    def collection(self):
        if self._collection is None:

            try:
                self._collection=self.client.get_collection(self.collection_id)
                self.apicalls+=1
            except APIError:
                raise APILimitReached(f"Collection {self.collection_id} not found or API limit reached")
        return self._collection


    def get_items(self,geom=None):
        """Get a dataframe of the items in the collection"""
        geoms=[]
        item_id=[]
        tstart=[]
        tend=[]
        if geom is None:
            searchitems=self.collection.get_items()
        else:
            # for some reason polygon query does not work so lets stick to bbox restriction
            searchitems=self.client.search(collections=[self.collection_id],bbox=geom.bounds).items()

            # searchitems=self.client.search(collections=self._collection,intersects=geom).items()
        for item in searchitems:
            geoms.append(shapely.from_geojson(json.dumps(item.geometry)))
            item_id.append(item.id)
            tstart.append(item.common_metadata.start_datetime)
            tend.append(item.common_metadata.end_datetime)

        gdf=gpd.GeoDataFrame(dict(tstart=tstart,tend=tend,item_id=item_id),geometry=geoms,crs="EPSG:4326")
        if geom is not None:
            gdf=gdf[gdf.geometry.within(geom)]

        return gdf

    def get_asset(self,item_id):
        #get the first asset (only) and download the data from the url
        try:
            firstasset=next(iter(self.collection.get_item(item_id).assets.values()))
            self.apicalls+=1
            req=requests.get(firstasset.href,headers=self.headers) 
            self.apicalls+=1
            df=self.readasset(io.StringIO(req.text))
        except:
            raise APILimitReached(f"Reached API limit {self.apicalls} for hydroweb-next?")

        return df

        



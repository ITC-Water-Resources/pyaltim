"""This script holds some functions to download and clean DAHITI Virtual Stations data records for any study area using study area shapefile or geojson file.
As a output of these functions, two different folders are created, DAHITI_Raw and DAHITI_Processed, for ideal management of raw and processed metadata files. A part of this function is adapted from https://dahiti.dgfi.tum.de/en/ api requesting example enlisted in their website.
"""

import geopandas as gpd
import pandas as pd
import os
import json
import requests
from pyaltim.core.logging import altlogger as log
from shapely import Point
import numpy as np
import xarray as xr
from datetime import datetime
from pyaltim.portals.api import APILimitReached
import getpass

class DahitiConnect:
    rooturl="https://dahiti.dgfi.tum.de/api/v2/"
    def __init__(self,apikey=None):
        if apikey is None:
            apikey=getpass.getpass("Please input your Dahiti v2 API v2 key")
        self.argsbase=dict(api_key=apikey)

    def list_targets(self,geom=None):
    
        if geom is None:
            #global coverage
            args={}
            args['min_lon'] = -180.0
            args['max_lon'] = 180.0
            args['min_lat'] = -90.0
            args['max_lat'] = 90.0
        else:
            # restrict query based on bounding box
            args={ky:geom.bounds[i] for i,ky in enumerate(['min_lon','min_lat','max_lon','max_lat'])}
        
        targets=self._handle_resp("list-targets",args)['data']
        exportkeys=[ky for ky in targets[0].keys() if ky not in ['longitude','latitude','data_access']]


        dfdict={colky:[val[colky] for val in targets] for colky in exportkeys}
        # only keep valid data_access prodcuts
        dfdict['data_access']=[[f"{ky}:{da}" for ky,da in target['data_access'].items() if da is not None] for target in targets]
        # make shapely points for the locations
        targetpoints=[Point(data['longitude'],data['latitude']) for data in targets] 

        gdftargets= gpd.GeoDataFrame(dfdict,geometry=targetpoints)
        # get rid of entries which have no data_access product
        # gdftargets=gdftargets[gdftargets.data_access != []]
        if geom is not None:
            #apply a second selection criteria (within the actual geometry not just the bounding box
            gdftargets=gdftargets[gdftargets.within(geom)]
        gdftargets=gdftargets.explode('data_access')
        gdftargets.set_crs('EPSG:4326',inplace=True)
        return gdftargets
   
    def get_waterlevel(self,dah_id):
        args={"format":"json","dahiti_id":dah_id}
        waterlevel=self._handle_resp("download-water-level",args)
        if len(waterlevel['data']) == 0:
            raise RuntimeError("No data found")
        
        ds=xr.Dataset(dict(water_level=('time',[val['water_level'] for val in waterlevel['data']]),wl_err=('time',[val['error'] for val in waterlevel['data']])),coords=dict(time=('time',[val['datetime'] for val in waterlevel['data']])))
        return waterlevel['info'],ds
        # df=pd.DataFrame(dict(time=[np.datetime64(val['datetime']) for val in waterlevel['data']],water_level=[val['water_level'] for val in waterlevel['data']],wl_err=[val['error'] for val in waterlevel['data']]))
        # return waterlevel['info'],df

    def get_by_product(self,dah_id,prodname):
        if prodname == "water_level_altimetry":
            return self.get_waterlevel(dah_id)
        else:
            log.error(f"Dahiti product name {prodname} not implemented")

    def _handle_resp(self,apipath,args):
        url=self.rooturl+apipath
        if not apipath.endswith("/"):
            url+="/"

        #add api key for authentication
        args.update(self.argsbase)
        
        response=requests.get(url,json=args)

        if response.status_code == 200:
            data = json.loads(response.text)
            # import pdb;pdb.set_trace()
            return data
        elif response.status_code == 429:
            raise APILimitReached(f"Dahiti API rate limit reached {response.text}")
        else:

            log.warning(response.text)
            log.warning(response.status_code)
    


"""This script holds some functions to download and clean DAHITI Virtual Stations data records for any study area using study area shapefile or geojson file.
As a output of these functions, two different folders are created, DAHITI_Raw and DAHITI_Processed, for ideal management of raw and processed metadata files. A part of this function is adapted from https://dahiti.dgfi.tum.de/en/ api requesting example enlisted in their website.
"""

import geopandas as gpd
import pandas as pd
import os
import requests
from pyaltim.core.logging import altlogger as log
from shapely import Point
import numpy as np
import xarray as xr
from datetime import datetime
from pyaltim.portals.api import APILimitReached,APIDataNotFound,APIOtherError
import getpass
from html.parser import HTMLParser
from io import StringIO
from gzip import GzipFile
import re

dlookup={'1':"SWE",'2':"WL",'3':"RD",'4':"WSch"}
#note: png color names do not match actual colors
iconlookup={"cyan.png":"SWE","red.png":"WL","blue.png":"RD","violet.png":"WSch","violet_ring.png":"WSch"}

class HydrosatHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.gdfinvent=None
        self.isscript=False
        self.df_search=pd.DataFrame(columns=['hyd_no','current_id','data_type','source_id'])
    
    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self.isscript=True
        elif tag == "a":
            #add hydrosat <-> current_id linkage
            if len(attrs) == 2 and attrs[0] == ('class', 'link'):
                href=attrs[1][1]
                #extract title and hydrosat id
                current_id=int(re.sub(r'^.+current=([0-9]+)&.+$',r'\1',href))
                hyd_no=int(re.sub(r'^.+hyd_no=([0-9]+)$',r'\1',href))
                data_type=dlookup[re.sub(r'^.+d_content=([0-9])&source=.+$',r'\1',href)]
                
                source_id=int(re.sub(r'^.+source=([0-9]+).+$',r'\1',href))
                self.df_search.loc[len(self.df_search)]=[hyd_no,current_id,data_type,source_id]

    def handle_endtag(self, tag):
        if tag == "script":
            self.isscript=False

    def handle_data(self, data):
        if not self.isscript:
            return
        if not 'var markers0 =' in data[0:20]:
            return 
        dataio=StringIO(data)
        line=dataio.readline()
        targets=[]
        while line != "":
            if 'var marker = new google.maps.Marker' in line:
                #extract the json
                jsonstr="{"
                for i in range(4):
                    line=dataio.readline()
                 
                    jsonstr+=line
                jsonstr+='}'
                #samitinize the json string
                jsonstr=re.sub(r'map: map,' , '',jsonstr)
                jsonstr=jsonstr.replace('\t','').replace('\n','')#
                title=re.sub(r'^.+title:[\s]+(\S+)[,\s].+$',r'\1',jsonstr).replace("'","").encode('utf-8')
                if title.endswith(b','):
                    #possible strip comma
                    title=title[:-1]
                # title=re.sub(r'^.+title: +(\S+),.+$',r'\1',jsonstr).replace("'","").encode('utf-8')
                icon=re.sub(r"^.+icon: '../images/(\S+)'.+$",r'\1', jsonstr)

                lat=float(re.sub(r'^.+lat: (\-?[0-9\.]+),.+$',r'\1',jsonstr))
                lon=float(re.sub(r'^.+lng: (\-?[0-9.]+)}.+$',r'\1',jsonstr))
                for i in range(3):
                    line=dataio.readline()
                #extract current_id
                current_id=int(re.sub(r'^.+current=([0-9]+).+$\n',r'\1',line))

                source_id=int(re.sub(r'^.+source=([0-9]+).+$',r'\1',line))
                locdict=dict(title=title,current_id=current_id,data_type=iconlookup[icon],source_id=source_id)
                # create shapely point
                locdict['geometry']=Point(lon,lat)

                targets.append(locdict)
            line=dataio.readline()

        if len(targets) > 0:
            self.gdfinvent=gpd.GeoDataFrame(targets)

class HydrosatConnect:
    """Class to connect to the Hydrosat website and retrieve data

    Attributes
    ----------
    rooturl : 

    """
    rooturl="https://hydrosat.gis.uni-stuttgart.de"
    def __init__(self,user=None,passw=None,cachedir=None):
        if cachedir is None:
            self.cachedir='hydrosat_cache'
        else:
            self.cachedir=cachedir
        
        if user is None:
            user=input("Please input your Hydrosat username")
        if passw is None:
            passw=getpass.getpass("Please input your Hydrosat password")
        self.user=user
        self.passw=passw

        self.cookies={}
        if self.user is not None and self.passw is not None:
            self.cookies=self.login()
    
        #possibly load cached inventory
        self.cacheinvent=os.path.join(self.cachedir,"Hydrosat_holdings.gpkg")
        if os.path.exists(self.cacheinvent):
            self.gdfinvent=gpd.read_file(self.cacheinvent)
        else:
            #empty version 
            self.gdfinvent=None
    
    def login(self):
        """Login to the Hydrosat website"""
        url=self.rooturl+"/php/ajax.php?r=200"
        resp=requests.post(url,data={"email":self.user,"pass":self.passw},verify=False)
        if resp.status_code != 200:
            raise APIOtherError(f"Login failed with status code {resp.status_code}")
        self.cookies=resp.cookies.get_dict() 
        return self.cookies
    
    def save_inventory(self,fgpkg=None):
        if fgpkg is None:
            fgpkg=self.cacheinvent
        if self.gdfinvent is None:
            raise RuntimeError("No inventory to save")
        self.gdfinvent.to_file(fgpkg,driver="GPKG")
    
    def refresh_inventory(self,geom=None,save=True):
        """
        List available water level targets
        Parameters
        ----------
        geom : possible geometry to constrain the search on 
            
        returns:
            A geopandas dataframe with the target information
        """
        #retrieve the complete map of the water level holdings (d_content=2)
        # url=self.rooturl+"/php/maps.php?d_content=2&source=1"
        url=self.rooturl+"/php/index.php"
        fcache=os.path.join(self.cachedir,"hydrosat.html")
        hydrosatparser=HydrosatHTMLParser()
        if os.path.exists(fcache) and os.path.getmtime(fcache) > datetime.now().timestamp()-86400:
            #renew catalogue
            renew=False
        else:
            renew=True

        if renew:
            resp=requests.get(url,verify=False)
            if resp.status_code == 200:
                with open(fcache,"w") as f:
                    f.write(resp.text)
            hydrosatparser.feed(resp.text)
        else:
            with open(fcache,"r") as f:
                hydrosatparser.feed(f.read())
        
        fcache_search=os.path.join(self.cachedir,"hydrosat_search.html")
        url_search=self.rooturl+"/php/ajax.php?r=4.2&title="
        if os.path.exists(fcache_search) and os.path.getmtime(fcache_search) > datetime.now().timestamp()-86400:
            #renew catalogue
            renew=True
        else:
            renew=False

        if renew:
            resp=requests.get(url_search,verify=False)
            if resp.status_code == 200:
                with open(fcache_search,"w") as f:
                    f.write(resp.text)
            hydrosatparser.feed(resp.text)
        else:
            with open(fcache_search,"r") as f:
                hydrosatparser.feed(f.read())
       
        #join the two dataframes on the current_id
        gdfinvent_combined=pd.merge(hydrosatparser.df_search,hydrosatparser.gdfinvent, on=['current_id','data_type','source_id'],how='inner')
        self.gdfinvent=gpd.GeoDataFrame(gdfinvent_combined,geometry='geometry',crs=4326)
        if save:
            self.save_inventory()
        return self.gdfinvent
    
    def list_targets(self,geom=None,data_type=None):
        if self.gdfinvent is None:
            self.refresh_inventory(geom)

        if geom is None and data_type is None:
            #no restrictions return entire catalogue
            return self.gdfinvent
        elif geom is not None:
            #apply a geometrical restriction
            return self.gdfinvent[self.gdfinvent.within(geom)]
        elif data_type is not None:
            return self.gdfinvent[self.gdfinvent.data_type == data_type]
        else:
            return self.gdfinvent[self.gdfinvent.within(geom) & (self.gdfinvent.data_type == data_type)]

    def parse_hydrosat_txt(self,gztxtfile):
        time=[]
        data=[]
        error=[]
        header=[]
        with GzipFile(gztxtfile,"r") as fid:
            for line in fid.readlines():
                line=line.decode('utf-8').lstrip().replace('\n','')
                if line.startswith('#'):
                    header.append(line)
                elif line != '':
                    if "NaN" in line:
                        continue
                    lnspl=line.split(',')
                    time.append(datetime(int(lnspl[0]),int(lnspl[1]),int(lnspl[2])).isoformat())
                    data.append(float(lnspl[3]))
                    error.append(float(lnspl[4]))
                    

        #parse header info
        headerdict={}
        for hdrline in header:
            kyval=hdrline[1:].split(":")
            if len(kyval) == 2:
                headerdict[kyval[0].lstrip()]=kyval[1].lstrip()

        dkey=headerdict['Data set content'].replace(' ','_').lower()
        dekey=dkey+"_err"
        ds=xr.Dataset({dkey:('time',data),dekey:error},coords=dict(time=('time',time)),attrs=headerdict)
        return headerdict,ds
                
    def get_by_product(self,hyd_no,prodname):
        if self.gdfinvent is None:
            self.refresh_inventory()
        
        #retrieve the data for a specific hydrosat product
        target=self.gdfinvent[(self.gdfinvent.hyd_no == hyd_no) &(self.gdfinvent.data_type == prodname)]
        if len(target) == 0:
            raise APIDataNotFound(f"No valid target found for {hyd_no} and {prodname}")

        #retrieve data
        #e.g. https://hydrosat.gis.uni-stuttgart.de/data/download/21111810572003.txt
        url=self.rooturl+f"/data/download/{hyd_no}.txt"
        fout=os.path.join(self.cachedir,f"{hyd_no}.gz")
        if os.path.exists(fout) and os.path.getmtime(fout) > datetime.now().timestamp()-86400:
            #renew data
            renew=False
        else:
            renew=True
        
        if renew:
            #download data
            resp=requests.get(url,verify=False,cookies=self.cookies)
            if resp.status_code == 404:
                raise APIDataNotFound(f"No data found for {hyd_no}")
            elif resp.status_code != 200:
                breakpoint()
                raise APIOtherError(f"Failed to retrieve data from {url}")
            with GzipFile(fout,"w") as fid:
                fid.write(resp.content)
        #parse the data into a xarray dataset and metadata
        header,ds=self.parse_hydrosat_txt(fout)
        return header,ds

        

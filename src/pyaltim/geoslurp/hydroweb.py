## Entry point to store and manage hydroweb data in a geoslurp enabled database 

from geoslurp.dataset import DataSet
from pyaltim.core.logging import altlogger 
from pyaltim.portals.hydroweb import HydrowebConnect
from geoslurp.dataset.pandasbase import PandasBase
from glob import glob
import os
import numpy as np

from sqlalchemy import Column, Integer,String
from sqlalchemy.dialects.postgresql import TIMESTAMP,JSONB
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from geoslurp.types.json import DataArrayJSONType
schema="pyaltim"
import geopandas as gpd 
import xarray as xr

class HydrowebBase(PandasBase):
    schema=schema
    product=None
    ftype="GPKG" 
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.pdfile=os.path.join(self.cacheDir(),"Hydroweb_holdings.gpkg")

    def pull(self,geom=None):
        if self.product is None:
            raise RuntimeError("Derived type of Hydrowebbase needs the prdouct member to be set")
        cred=self.conf.authCred("hydroweb_next",qryfields=["apikey"])
        hywconn=HydrowebConnect(collection_id=self.product,apikey=cred.apikey)
        altlogger.info(f"Cataloging items for {self.product}" )
        gdfhyweb=hywconn.get_items() 
        gdfhyweb.to_file(self.pdfile,driver="GPKG")
    

@as_declarative(metadata=MetaData(schema=schema))
class HydrowebTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id=Column(Integer,primary_key=True)
    item_id=Column(String,index=True,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    data=Column(DataArrayJSONType)


class HydrowebAssetBase(DataSet):
    product=None
    schema=schema
    holdingcls=None
    def __init__(self,dbconn):
        if self.product is None:
            raise RuntimeError("class product member needs to be described in derived class")
        super().__init__(dbconn)
        self.holdings=self.holdingcls(dbconn)

    def pull(self):
        altlogger.info("Updating Hydroweb holdings")
        self.holdings.pull()
        self.holdings.register()

    def register(self,geom=None):
        if self.db.tableExists(self.stname()):
            # lastupdate=self.dahtargets._dbinvent.lastupdate.isoformat()
            # only select stations which require updating (lastupdate < catalogue update)
            qry=f"SELECT comb.item_id,comb.geometry FROM (SELECT targets.*, prod.lastupdate FROM {self.holdingcls.stname()} as targets LEFT JOIN {self.stname()} as prod ON targets.item_id = prod.item_id) AS comb WHERE comb.lastupdate IS NULL OR comb.lastupdate < comb.tend"
        else:
            qry=f"SELECT * from {self.holdingcls.stname()}"
        dftargets=gpd.read_postgis(qry,self.db.dbeng,geom_col="geometry")
        if geom is not None:
            #select only a subset of the data to download
            dftargets=dftargets[dftargets.within(geom)]        
        
        if len(dftargets) == 0:
            altlogger.info("nothing to update/register")

        cred=self.conf.authCred("hydroweb_next",qryfields=["apikey"])
        hywconn=HydrowebConnect(collection_id=self.product,apikey=cred.apikey)
        altlogger.info(f"retrieving assets for {self.product}" )
        nfail=0
        for ix,darow in dftargets.iterrows():
            altlogger.info(f"getting {self.product} for {darow['item_id']}")
            info,dsprod=hywconn.get_asset(darow['item_id'])
            proddict={ky:val for ky,val in info.items() if ky in ["lastupdate","tstart","tend"]}
            proddict["item_id"]=darow['item_id']
            proddict['data']=dsprod
            #create a dictionary to upsert in the table
            
            self.upsertEntry(proddict,index_elements=['item_id'])



def getHydroWebDsets(conf):
    """Generate relevant Hydroweb datasets"""
    clss=[] 
    for product in HydrowebConnect.products:
        clsName=product.lower()
        clsshold=type(clsName, (HydrowebBase,), {"product":product})
        clss.append(clsshold)
        # also create an assets table
        clsName2=product.lower()+"_assets"
        qtable=type(clsName2 +"Table", (HydrowebTBase,), {})
        clss.append(type(clsName2, (HydrowebAssetBase,), {"product":product,"holdingcls":clsshold,"table":qtable}))

    return clss




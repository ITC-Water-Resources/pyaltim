## Entry point to store and manage hydroweb data in a geoslurp enabled database 

from geoslurp.dataset.pandasbase import PandasBase
from geoslurp.dataset.dataSetBase import DataSet
from sqlalchemy.sql.sqltypes import BigInteger
from pyaltim.core.logging import altlogger
from pyaltim.portals.hydrosat import HydrosatConnect
from glob import glob
import geopandas as gpd
import os
import numpy as np
from datetime import datetime
from sqlalchemy import Column, Integer,BigInteger
from sqlalchemy.dialects.postgresql import TIMESTAMP,JSONB
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from geoslurp.types.json import DataArrayJSONType
from pyaltim.portals.api import APILimitReached,APIDataNotFound

schema="pyaltim"

class HydrosatTargets(PandasBase):
    schema=schema
    ftype="GPKG" 
    def __init__(self,dbconn):
        super().__init__(dbconn)
        #overwrite cachedir
        self.setCacheDir(self.conf.getCacheDir(self.schema,'HydroSat'))
        self.pdfile=os.path.join(self.cacheDir(),'Hydrosat_holdings.gpkg') 
    
    def pull(self):
        #retrieve apikey and output directory to replace in the configuration
        
        cred=self.conf.authCred("hydrosat",qryfields=["user","passw"])
        hysatcon=HydrosatConnect(cred.user,cred.passw,cachedir=self.cacheDir())
        
        #retrieve the complete catalogue first
        altlogger.info("Downloading current Hydrosat holdings")
        gdfhysat=hysatcon.refresh_inventory()

        #save current holdings to a cached GPKG file
        hysatcon.save_inventory()



@as_declarative(metadata=MetaData(schema=schema))
class HydrosatTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id=Column(Integer,primary_key=True)
    hyd_no=Column(BigInteger,index=True,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    source_id=Column(Integer)
    header=Column(JSONB)
    data=Column(DataArrayJSONType)

class HydrosatBase(DataSet):
    product=None
    schema=schema
    def __init__(self,dbconn):
        if self.product is None:
            raise RuntimeError("class product member needs to be described in derived class")
        super().__init__(dbconn)
        #use the same cache directory as the inventory
        self.setCacheDir(self.conf.getCacheDir(self.schema,'HydroSat'))
        self.hydrosat_targets=HydrosatTargets(dbconn)
        
    def pull(self):
        #Update the Dahititargets table
        altlogger.info("Updating Hydrosat holdings")
        self.hydrosat_targets.pull()
        self.hydrosat_targets.register()

    def register(self,geom=None):
        if self.db.tableExists(self.stname()):
            lastupdate=self.hydrosat_targets._dbinvent.lastupdate.isoformat()
            # only select stations which require updating (lastupdate < catalogue update)
            qry=f"SELECT comb.hyd_no, comb.lastupdate,comb.data_type,comb.source_id, comb.geometry FROM (SELECT targets.hyd_no, prod.lastupdate, targets.data_type, targets.source_id, targets.geometry FROM {schema}.hydrosattargets as targets LEFT JOIN {self.stname()} as prod ON targets.hyd_no = prod.hyd_no) AS comb WHERE comb.lastupdate IS NULL OR comb.lastupdate < '{lastupdate}'"
        else:
            qry=f"SELECT * from {schema}.hydrosattargets"
        dftargets=gpd.read_postgis(qry,self.db.dbeng,geom_col="geometry")
        if geom is not None:
            #select only a subset of the data to download
            dftargets=dftargets[dftargets.within(geom)]        
        #select only relevant products
        dftargets=dftargets[dftargets.data_type == f"{self.product}"]
        if len(dftargets) == 0:
            altlogger.info("nothing to update/register")
        ncount=0
        cred=self.conf.authCred("hydrosat",qryfields=["user","passw"])
        hysatcon=HydrosatConnect(cred.user,cred.passw,cachedir=self.cacheDir())
        for ix,hysatrow in dftargets.iterrows():
            altlogger.info(f"getting {self.product} for {hysatrow['hyd_no']}")
            try:
                header, dsprod=hysatcon.get_by_product(hysatrow['hyd_no'],self.product)
            except APIDataNotFound as exc:
                altlogger.warning(f"No data found for {hysatrow['hyd_no']},continuing")
                continue
            except APILimitReached:
                altlogger.warning(f"APILimitReached, stopping")
                break

            #create a dictionary to upsert in the table

            proddict=dict(hyd_no=hysatrow['hyd_no'],tstart=dsprod.time.min().item(), tend=dsprod.time.max().item(),source_id=hysatrow['source_id'],header=header,data=dsprod,lastupdate=datetime.now())
            
            self.upsertEntry(proddict,index_elements=['hyd_no'])
            ncount+=1




def getHydroSatDsets(conf):
    # """Generate relevant Hydrosat datasets"""
    products=["WL"]

    clss=[HydrosatTargets] 
    for product in products:
        clsName="hydrosat_"+product.lower()
        qtable=type(clsName +"Table", (HydrosatTBase,), {})
        clss.append(type(clsName, (HydrosatBase,), {"product":product,"table":qtable}))

    return clss

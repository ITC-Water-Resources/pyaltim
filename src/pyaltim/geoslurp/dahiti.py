## Entry point to store and manage hydroweb data in a geoslurp enabled database 

from geoslurp.dataset.pandasbase import PandasBase
from geoslurp.dataset.dataSetBase import DataSet
from geoslurp.config.slurplogger import slurplog
from pyaltim.portals.dahiti import DahitiConnect
from glob import glob
import geopandas as gpd
import os
import numpy as np
from datetime import datetime
from sqlalchemy import Column, Integer,String
from sqlalchemy.dialects.postgresql import TIMESTAMP,JSONB
from sqlalchemy.ext.declarative import declared_attr, as_declarative
from sqlalchemy import MetaData
from geoslurp.types.json import DataArrayJSONType
from pyaltim.portals.api import APILimitReached

schema="pyaltim"

class DahitiTargets(PandasBase):
    schema=schema
    ftype="GPKG" 
    def __init__(self,dbconn):
        super().__init__(dbconn)
        self.pdfile=os.path.join(self.cacheDir(),'Dahiti_holdings.gpkg') 
    
    def pull(self):
        #retrieve apikey and output directory to replace in the configuration
        
        cred=self.conf.authCred("dahitiv2",qryfields=["apikey"])
        dahcon=DahitiConnect(cred.apikey)
        
        #retrieve the complete catalogue first
        slurplog.info("Downloading current Dahiti holdings")
        gdfdahiti=dahcon.list_targets()
        #save to a cached GPKG file
        gdfdahiti.to_file(self.pdfile,driver="GPKG")


@as_declarative(metadata=MetaData(schema=schema))
class DahitiTBase(object):
    @declared_attr
    def __tablename__(cls):
        #strip of the 'Table' from the class name
        return cls.__name__[:-5].replace("-","_").lower()
    id=Column(Integer,primary_key=True)
    dahiti_id=Column(Integer,index=True,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP,index=True)
    tend=Column(TIMESTAMP,index=True)
    data=Column(DataArrayJSONType) 

class DahitiBase(DataSet):
    product=None
    schema=schema
    def __init__(self,dbconn):
        if self.product is None:
            raise RuntimeError("class product member needs to be described in derived class")
        super().__init__(dbconn)
        self.dahtargets=DahitiTargets(dbconn)
        
    def pull(self):
        #Update the Dahititargets table
        slurplog.info("Updating dahiti holdings")
        #dahtargets=DahitiTargets(self.db)
        self.dahtargets.pull()
        self.dahtargets.register()

    def register(self,geom=None):
        if self.db.tableExists(self.stname()):
            lastupdate=self.dahtargets._dbinvent.lastupdate.isoformat()
            # only select stations which require updating (lastupdate < catalogue update)
            qry=f"SELECT comb.dahiti_id, comb.lastupdate,comb.data_access,comb.geometry FROM (SELECT targets.dahiti_id, prod.lastupdate,targets.data_access, targets.geometry FROM {schema}.dahititargets as targets LEFT JOIN {self.stname()} as prod ON targets.dahiti_id = prod.dahiti_id) AS comb WHERE comb.lastupdate IS NULL OR comb.lastupdate < '{lastupdate}'"
        else:
            qry=f"SELECT * from {schema}.dahititargets"
        dftargets=gpd.read_postgis(qry,self.db.dbeng,geom_col="geometry")
        if geom is not None:
            #select only a subset of the data to download
            dftargets=dftargets[dftargets.within(geom)]        
        
        #select only relevant products
        dftargets=dftargets[dftargets.data_access == f"{self.product}:public"]
        if len(dftargets) == 0:
            slurplog.info("nothing to update/register")

        cred=self.conf.authCred("dahitiv2",qryfields=["apikey"])
        dahcon=DahitiConnect(cred.apikey)
        for ix,darow in dftargets.iterrows():
            slurplog.info(f"getting {self.product} for {darow['dahiti_id']}")
            target, dsprod=dahcon.get_by_product(darow['dahiti_id'],self.product)

                
            #create a dictionary to upsert in the table
            proddict=dict(dahiti_id=darow['dahiti_id'],tstart=dsprod.time.min().item(), tend=dsprod.time.max().item(),data=dsprod,lastupdate=datetime.now())
            
            
            self.upsertEntry(proddict,index_elements=['dahiti_id'])



def getDahitiDsets(conf):
    # """Generate relevant Hydroweb datasets"""
    products=["water_level_altimetry"]

    clss=[DahitiTargets] 
    for product in products:
        clsName="dahiti_"+product.lower()
        qtable=type(clsName +"Table", (DahitiTBase,), {})
        clss.append(type(clsName, (DahitiBase,), {"product":product,"table":qtable}))

    return clss




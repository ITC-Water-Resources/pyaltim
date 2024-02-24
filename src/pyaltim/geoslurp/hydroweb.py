## Entry point to store and manage hydroweb data in a geoslurp enabled database 

from geoslurp.dataset import DataSet
from geoslurp.config.slurplogger import slurplog
from eodag import EODataAccessGateway, SearchResult
from pyaltim.io.hydroweb_io import readHydroWeb_Lakes,readHydroWeb_Rivers
from glob import glob
import os
import numpy as np

theiayaml="""
hydroweb_next:
    priority: 10
    auth:
        credentials:
            apikey: APIKEYREPLACE
    download:
        outputs_prefix: OUTPUTPATHREPLACE
"""

class HydrowebBase(DataSet):
    schema="pyaltim"
    product=None
    readfunc=None
    def __init__(self,dbconn):
        super().__init__(dbconn)

    
    def pull(self,geom=None):
        if self.product is None:
            raise RuntimeError("Derived type of Hydrowebbase needs the prdouct member to be set")
        # geom="POLYGON ((32.11853 -1.74793,34.75525 -1.74793,34.75525 -0.30212,32.11853 -0.30212,32.11853 -1.74793))" 
        
        # geom="POLYGON ((31.83165 -3.31937,46.74719 -3.31937,46.74719 4.60545,31.83165 4.60545,31.83165 -3.31937))"
        if geom is None:
            raise RuntimeError("geometry needs to be provided")
        #retrieve apikey and output directory to replace in the configuration
        
        cachedir=self.cacheDir()
        cred=self.conf.authCred("hydroweb_next",qryfields=["apikey"])
        ymlconfig=theiayaml.replace('APIKEYREPLACE',cred.apikey).replace('OUTPUTPATHREPLACE',cachedir)
              
        dag = EODataAccessGateway()
        dag.update_providers_config(ymlconfig)
        #Search the catalogue 

        search_results= dag.search_all(productType=self.product, geom=geom)
        
        slurplog.info(f"Found {len(search_results)} {self.product} products, start download") 
        dag.download_all(search_results)
    
    def register(self):
        # retrieve files in the cache directory
        self.dropTable()
        searchpath=self.cacheDir()+"/*/*.txt"
        cachedfiles=[file for file in glob(searchpath)]
        dfcomplete=None 
        for file in cachedfiles:
            slurplog.info(f"Adding {os.path.basename(file)} to {self.schema}.{self.name}") 
            df=self.readfunc(file)
            tname=self.name
            df.to_postgis(name=tname,con=self.db.dbeng,if_exists="append",schema=self.schema,index_label="hydrowebid")

        #also update entry in the inventory table
        self.updateInvent()

def getHydroWebDsets(conf):
    """Generate relevant Hydroweb datasets"""
    products=["HYDROWEB_RIVERS","HYDROWEB_LAKES"]
    readf={"HYDROWEB_LAKES":readHydroWeb_Lakes,"HYDROWEB_RIVERS":readHydroWeb_Rivers}
    clss=[] 
    for product in products:
        clsName=product.lower()
        clss.append(type(clsName, (HydrowebBase,), {"product":product,"readfunc":staticmethod(readf[product])}))

    return clss




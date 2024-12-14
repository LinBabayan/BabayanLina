`from abc import ABC, abstractmethod
import json



class DBAccessBase(ABC):
    def _constructADQLQuery(self, query_params_json, limit, chunk_size) -> str: #protected(_)
       pass
        
    #Form ADQL query, request it to the catalog API, process response and return result
    @abstractmethod
    def QueryCatalog(self, query_params_json, limit, chunk_size):
        pass

    #collection of Categories supported by DBAccess
    @property
    @abstractmethod
    def CategoryInfo(self):
         pass

    #Catalog name
    @property
    @abstractmethod
    def Catalog(self):
         pass

    #Id column name in resultset
    @property
    @abstractmethod
    def ColumnId(self):
         pass

    #Epoch used for the catalog
    @property
    def Epoch(self):
        return 0  # Undefined

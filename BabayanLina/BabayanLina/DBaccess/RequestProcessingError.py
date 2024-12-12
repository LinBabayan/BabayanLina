class RequestProcessingError(Exception):
    def __init__(self, catalog: str, query: str, status:str, message: str):
        super().__init__(message)
        self.catalog = catalog
        self.query = query
        self.status = status

class CrossMatchRequestProcessingError(Exception):
    def __init__(self, catalog_source: str, query: str, catalog_to_match: str, crossmatch_query: str,  status:str, message: str):
        super().__init__(message)
        self.catalog_source = catalog_source
        self.query = query
        self.catalog_to_match = catalog_to_match
        self.crossmatch_query = crossmatch_query
        self.status = status
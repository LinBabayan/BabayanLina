from .DBAccessBase import DBAccessBase
from DBaccess.RequestProcessingError import RequestProcessingError

# SDSS API endpoint
SDSS_Url = "http://skyserver.sdss.org/dr16/SkyServerWS/SearchTools/TapSearch"
HTTP_Headers = {
    "Content-Type": "application/x-www-form-urlencoded",
}
class DBAccessSDSS(DBAccessBase):
    def QueryCatalog(self, query_params, limit, chunk_size):
        raise RequestProcessingError(self.Catalog, "", f"Not supported", "error")

    @property
    def CategoryInfo(self):
        return {}

    @property
    def Catalog(self):
        return "sdss"

    @property
    def ColumnId(self):
        return "sdss_id"

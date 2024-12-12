import requests
from astropy.io.votable import parse
from io import BytesIO
from .DBAccessBase import DBAccessBase
from DBaccess.RequestProcessingError import RequestProcessingError
from .DBAccessEnums import Category, ObjectTypes

# Gaia API endpoint
Gaia_Url = "https://gea.esac.esa.int/tap-server/tap/sync"

HTTP_Headers = {
    "Content-Type": "application/x-www-form-urlencoded",
}


class DBAccessGaia(DBAccessBase):
    def _constructADQLQuery(self, query_params_json, limit, chunk_size) -> str:
        query_params_json = {k.lower(): v for k, v in query_params_json.items()}

        from_clause = "gaiadr3.gaia_source AS gs\n"
        where_clause = ""

        table_aliases = []
        #form FROM and WHERE clauses depending on specified object type
        if query_params_json.get('object_types'):
            object_types = query_params_json['object_types']
            object_types = object_types.lower()

            if object_types == ObjectTypes.Star.value.lower():
                table_aliases = ["gs", "obj"]
                from_clause += "\tINNER JOIN gaiadr3.astrophysical_parameters AS obj ON obj.source_id = gs.source_id"
                #where_clause = self._addConditionToWhere(where_clause, " obj.mass_flame IS NOT NULL")

            elif object_types == ObjectTypes.Galaxy.value.lower():
                table_aliases = ["gs"]
                where_clause = self._addConditionToWhere(where_clause," gs.classprob_dsc_combmod_galaxy > 0.8 AND ABS(gs.parallax) < 0.1 AND ABS(gs.pmra) < 0.1 AND ABS(gs.pmdec) < 0.1")

            elif object_types == ObjectTypes.Quasar.value.lower():
                table_aliases = ["gs"]
                where_clause = self._addConditionToWhere(where_clause," gs.classprob_dsc_combmod_quasar > 0.8 AND ABS(gs.parallax) < 0.1 AND ABS(gs.pmra) < 0.1 AND ABS(gs.pmdec) < 0.1")
            else:
                raise ValueError("Invalid object_types={object_types} is specified.")

        # form WHERE clauses depending on specified categories
        for category, field_data in self.CategoryInfo.items():
            category_name = category.name.lower()
            if field_data[1] in table_aliases:
                if query_params_json.get('min_' + category_name):
                    where_clause = self._addConditionToWhere(where_clause, f"{field_data[0]} >= {query_params_json['min_' + category_name]}")
                if query_params_json.get('max_' + category_name):
                    where_clause = self._addConditionToWhere(where_clause, f"{field_data[0]} <= {query_params_json['max_' + category_name]}")
                if category_name == Category.ObjectsInCircle.name.lower() and query_params_json.get(category_name):
                    objects_in_circle = query_params_json.get(category_name)
                    if (objects_in_circle.get(Category.RA.name.lower()) and
                        objects_in_circle.get(Category.Dec.name.lower()) and
                        objects_in_circle.get(Category.Radius.name.lower())):
                        circle_center_ra = objects_in_circle.get(Category.RA.name.lower())
                        circle_center_dec = objects_in_circle.get(Category.Dec.name.lower())
                        circle_center_radius = objects_in_circle.get(Category.Radius.name.lower())
                        where_clause = self._addConditionToWhere(where_clause,
                                            f"1=CONTAINS(POINT('ICRS', {self.CategoryInfo.get(Category.RA)[0]}, {self.CategoryInfo.get(Category.Dec)[0]}), CIRCLE('ICRS', {circle_center_ra}, {circle_center_dec}, {circle_center_radius}))")

        #if chunk_size

        #form SELECT clause
        select_criteria = ""
        if limit:
            select_criteria = f"TOP {limit}"
        select_clause = f"SELECT {select_criteria} gs.source_id AS {self.ColumnId}"
        for category, field_data in self.CategoryInfo.items():
            if  field_data[1] in table_aliases and field_data[0]:
                field_name = field_data[0]
                select_clause += f", {field_name} AS {category.name}"

        query = select_clause + "\n FROM " + from_clause
        if where_clause != "":
            query += "\n WHERE " + where_clause
        return query

    def QueryCatalog(self, query_params_json, limit, chunk_size):

        query = self._constructADQLQuery(query_params_json, limit, chunk_size)
        #query = "SELECT TOP 10 source_id, ra, dec FROM gaiadr3.gaia_source"
        params = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "QUERY": query,
            "FORMAT": "votable"
        }
        try:
            response = requests.post(Gaia_Url, data=params, headers=HTTP_Headers)
        except Exception as e:
            raise RequestProcessingError(self.Catalog, query,f"Error processing Gaia request", str(e))

        if response.status_code == 200:
            try:
                #print(response.headers)
                #print(response.text[:500])

                votable = parse(BytesIO(response.content))
                table = votable.get_first_table().to_table()

                columns = table.colnames
                rows = [dict(zip(columns, row)) for row in table]
                return self.Catalog, query, columns, rows, table

            except Exception as e:
                raise RequestProcessingError(self.Catalog, query, "API request failed", str(e))

        raise RequestProcessingError(self.Catalog, query, "API request failed",  f"HTTP error {response.status_code}: {response.text}")

    @property
    def CategoryInfo(self):
        return {
                #from gaiadr3.gaia_source AS gs
                Category.RA:            ['gs.ra', 'gs'],
                Category.Dec:           ['gs.dec', 'gs'],
                Category.PMRA:          ['gs.pmra', 'gs'],
                Category.PMDec:         ['gs.pmdec', 'gs'],
                Category.Parallax:      ['gs.parallax', 'gs'],
                Category.ProperMotion:  ['gs.pm', 'gs'],
                Category.GMagnitude:    ['gs.phot_g_mean_mag', 'gs'],
                Category.BPMagnitude:   ['gs.phot_bp_mean_mag', 'gs'],
                Category.RPMagnitude:   ['gs.phot_rp_mean_mag', 'gs'],
                Category.RadialVelocity:['gs.radial_velocity', 'gs'],
                Category.ObjectsInCircle:['','gs'],
                #from astrophysical_parameters AS obj, only for stars
                Category.Mass:          ['obj.mass_flame', 'obj'],
                Category.Radius:        ['obj.radius_flame', 'obj'],
                Category.Luminosity:    ['obj.lum_flame', 'obj'],
                Category.Temperature:   ['obj.teff_gspphot', 'obj'],
                Category.Gravity:       ['obj.logg_gspphot', 'obj'],
        }

    @property
    def Catalog(self):
        return "gaia"

    @property
    def ColumnId(self):
         return "gaia_id"

    # Epoch used for the catalog
    @property
    def Epoch(self):
          return "J2015.5" # Gaia epoch (J2015.5)
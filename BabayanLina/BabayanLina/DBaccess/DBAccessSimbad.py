from .DBAccessBase import DBAccessBase
from DBaccess.RequestProcessingError import RequestProcessingError
from .DBAccessEnums import Category, ObjectTypes

class DBAccessSimbad(DBAccessBase):
    def QueryCatalog(self, query_params_json, limit, chunk_size):
        def _constructADQLQuery(self, query_params_json, limit, chunk_size) -> str:
            query_params_json = {k.lower(): v for k, v in query_params_json.items()}

            from_clause = ("basic AS bs\n" +
                    "       INNER JOIN ident ON bs.oid = ident.oidref\n"
                    "       INNER JOIN otypedef ON bs.otype = otypedef.otype\n")
            where_clause = ""

            table_aliases = []
            # form FROM and WHERE clauses depending on specified object type
            if query_params_json.get('object_types'):
                object_types = query_params_json['object_types']
                object_types = object_types.lower()

                if object_types == ObjectTypes.Star.value.lower():
                    where_clause = self._addConditionToWhere(where_clause, " bs.otype LIKE '*%'")

                elif object_types == ObjectTypes.Galaxy.value.lower():
                    table_aliases = ["gs"]
                    where_clause = self._addConditionToWhere(where_clause,
                                                             " gs.classprob_dsc_combmod_galaxy > 0.8 AND ABS(gs.parallax) < 0.1 AND ABS(gs.pmra) < 0.1 AND ABS(gs.pmdec) < 0.1")

                elif object_types == ObjectTypes.Quasar.value.lower():
                    table_aliases = ["gs"]
                    where_clause = self._addConditionToWhere(where_clause,
                                                             " gs.classprob_dsc_combmod_quasar > 0.8 AND ABS(gs.parallax) < 0.1 AND ABS(gs.pmra) < 0.1 AND ABS(gs.pmdec) < 0.1")
                else:
                    raise ValueError("Invalid object_types={object_types} is specified.")

            # form WHERE clauses depending on specified categories
            for category, field_data in self.CategoryInfo.items():
                category_name = category.name.lower()
                if field_data[1] in table_aliases:
                    if query_params_json.get('min_' + category_name):
                        where_clause = self._addConditionToWhere(where_clause,
                                                                 f"{field_data[0]} >= {query_params_json['min_' + category_name]}")
                    if query_params_json.get('max_' + category_name):
                        where_clause = self._addConditionToWhere(where_clause,
                                                                 f"{field_data[0]} <= {query_params_json['max_' + category_name]}")
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

            # if chunk_size

            # form SELECT clause
            select_criteria = ""
            if limit:
                select_criteria = f"TOP {limit}"
            select_clause = (f'SELECT {select_criteria} bs.main_id AS {self.ColumnId}, ' 
                              'ident.id AS simbad_name, bs.otype AS simbad_otype, otypedef.description simbad_type_description,\n')
            for category, field_data in self.CategoryInfo.items():
                if field_data[1] in table_aliases and field_data[0]:
                    field_name = field_data[0]
                    select_clause += f", {field_name} AS {category.name}"

            query = select_clause + "\n FROM " + from_clause
            if where_clause != "":
                query += "\n WHERE " + where_clause
            return query

    @property
    def CategoryInfo(self):
        return {
            # from basic AS bs
            Category.RA: ['bs.ra', 'bs'],
            Category.Dec: ['bs.dec', 'bs'],
            Category.PMRA: ['bs.pmra', 'bs'],
            Category.PMDec: ['bs.pmdec', 'bs'],
            Category.Parallax: ['gs.plx_value', 'bs'],
            Category.ProperMotion: ['gs.pm', 'bs'],
            Category.GMagnitude: ['bs.g_mag', 'bs'],
            Category.BPMagnitude: ['bs.bp_mag', 'bs'],
            Category.RPMagnitude: ['bs.rp_mag', 'bs'],
            #Radial velocity in km/s
            Category.RadialVelocity: ['bs.rvz_radvel', 'bs'],
            Category.ObjectsInCircle: ['', 'bs'],
        }

    @property
    def Catalog(self):
        return "simbad"

    @property
    def ColumnId(self):
        return "simbad_id"

    # Epoch used for the catalog
    @property
    def Epoch(self):
        return "J2000.0" # SIMBAD epoch (J2000.0)
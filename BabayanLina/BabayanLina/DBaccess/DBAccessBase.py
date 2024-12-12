from abc import ABC, abstractmethod
import json



class DBAccessBase(ABC):
    def _constructADQLQuery(self, query_params_json, limit, chunk_size) -> str: #protected(_)
        query = "SELECT source_id, parallax, phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag, pmra, pmd, teff, radius, distance FROM gaia_source WHERE"

        conditions = []

        # Object types filters (e.g., stellar, galaxy, quasar)
        if query_params.get('object_types'):
            object_types = query_params['object_types']
            conditions.append(f"object_type IN ({','.join(object_types)})")

        # Categorical classification filters (e.g., spectral type, classification)
        if query_params.get('stellar_classification'):
            stellar_classification = query_params['stellar_classification']
            conditions.append(f"stellar_classification IN ({','.join(stellar_classification)})")

        if query_params.get('galaxy_classification'):
            galaxy_classification = query_params['galaxy_classification']
            conditions.append(f"galaxy_classification IN ({','.join(galaxy_classification)})")

        if query_params.get('spectral_type'):
            spectral_type = query_params['spectral_type']
            conditions.append(f"spectral_type IN ({','.join(spectral_type)})")

        # Numeric filters (e.g., magnitude, distance, temperature)
        if query_params.get('min_parallax'):
            conditions.append(f"parallax >= {query_params['min_parallax']}")
        if query_params.get('max_parallax'):
            conditions.append(f"parallax <= {query_params['max_parallax']}")

        if query_params.get('min_g_magnitude'):
            conditions.append(f"phot_g_mean_mag >= {query_params['min_g_magnitude']}")
        if query_params.get('max_g_magnitude'):
            conditions.append(f"phot_g_mean_mag <= {query_params['max_g_magnitude']}")

        if query_params.get('min_temperature'):
            conditions.append(f"teff >= {query_params['min_temperature']}")
        if query_params.get('max_temperature'):
            conditions.append(f"teff <= {query_params['max_temperature']}")

        # Join all conditions and form the full query
        query += " AND ".join(conditions)

        return query

    def _addConditionToWhere(self, where_clause, condition) -> str:
        if condition == "":
            return where_clause

        if where_clause != "":
            where_clause += " AND "
        where_clause += condition

        return where_clause

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
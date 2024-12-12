from abc import ABC
import pyvo
from astropy.table import join
from .DBAccessEnums import Category
from DBaccess.RequestProcessingError import RequestProcessingError
from DBaccess.RequestProcessingError import CrossMatchRequestProcessingError
from astropy.table import Table
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.time import Time

from .DBAccessBase import DBAccessBase
#from DBaccess.RequestProcessingError import RequestProcessingError

simbad_tap = pyvo.dal.tap.TAPService("http://simbad.u-strasbg.fr/simbad/sim-tap")

column_adjusted_ra = 'adjusted_ra'
column_adjusted_dec = 'adjusted_dec'


class CrossMatching(ABC):
    def CrossMatching(self, catalog_source: DBAccessBase, catalog_to_match: DBAccessBase, query_params_json, limit, chunk_size):
        try:
            catalog, query, gaia_columns, gaia_rows, table = catalog_source.QueryCatalog(query_params_json, limit, chunk_size)
        except RequestProcessingError as e:
            raise CrossMatchRequestProcessingError(e.catalog, e.query, catalog_to_match.Catalog, "", "Request to source catalog failed", str(e))

        # Extract gaia_id, "ra", "dec" to upload temporary table to Simbad
        crossmatch_query = ""

        try:
            #We need to adjust coordinates (RA and DEC) read from source to RA and DEC of catalog to match
            column_ra = self.__setColumnUnitToDeg(table[Category.RA.name])  # RA in degrees
            column_dec = self.__setColumnUnitToDeg(table[Category.Dec.name])  # Dec in degrees
            column_pmra = self.__setColumnUnitToMilliarcsecPerYear(table[Category.PMRA.name])  # Proper motion in RA (mas/yr)
            column_pmdec = self.__setColumnUnitToMilliarcsecPerYear(table[Category.PMDec.name])  # Proper motion in Dec (mas/yr)

            #obstime = Time("J2000.0")
            #new_obstime = Time("J2015.5")
            #coords = SkyCoord(
            #    ra=88.79293899077537 * u.deg,
            #    dec=7.407063995272694* u.deg,
            #    pm_ra_cosdec=self.__setColumnUnitToMilliarcsecPerYear(27.54),
            #    pm_dec=self.__setColumnUnitToMilliarcsecPerYear(11.3) ,
            #    frame="icrs",
            #    obstime=obstime
            #)
            #adjusted_coords = coords.apply_space_motion(new_obstime=new_obstime)

            # Create SkyCoord object for source
            obstime = Time(catalog_source.Epoch)
            coords = SkyCoord(
                ra=column_ra,
                dec=column_dec,
                pm_ra_cosdec=column_pmra,
                pm_dec=column_pmdec,
                frame="icrs",
                obstime=obstime
            )
            # Adjust source coordinates to catalog_to_match (for example, adjust Gaia coordinates to SIMBAD epoch )
            new_obstime = Time(catalog_to_match.Epoch)
            adjusted_coords = coords.apply_space_motion(new_obstime=new_obstime)

            # Add columns for adjusted RA and Dec to the table
            table[column_adjusted_ra] = adjusted_coords.ra.deg
            table[column_adjusted_dec] = adjusted_coords.dec.deg

        except Exception as e:
            raise CrossMatchRequestProcessingError(catalog_source.Catalog, query, catalog_to_match.Catalog, crossmatch_query, f"Error adjusting source coordinates to the target", str(e))

        try:
            # create table to upload to the target catalog with the columns source_id, adjusted_ra, adjusted_dec
            upload_table = table[catalog_source.ColumnId, column_adjusted_ra, column_adjusted_dec]

            # Define crossmatch query
            crossmatch_query = (" SELECT basic.main_id AS simbad_id,\n" +
                                "        ident.id AS simbad_name,\n"
                                "        basic.otype AS simbad_otype,\n"
                                "        otypedef.description simbad_type_description,\n"                               
                                "        basic.ra AS simbad_ra,\n"
                                "        basic.dec AS simbad_dec,\n"
                               f"        tmp_table.{catalog_source.ColumnId}\n"
                                "  FROM  basic \n"
                                "       INNER JOIN ident ON basic.oid = ident.oidref\n"
                                "       INNER JOIN otypedef ON basic.otype = otypedef.otype\n"
                               f"       INNER JOIN TAP_UPLOAD.tmp_table ON 1=CONTAINS(POINT('ICRS', basic.ra, basic.dec), CIRCLE('ICRS', tmp_table.{column_adjusted_ra}, tmp_table.{column_adjusted_dec}, 0.001))")
                                #"WHERE basic.otype IN ('Star', 'V*', 'SB*')")
                                #basic.otype IN ('Star', 'V*', 'SB*')

            # Submit the job: Upload source data (Gaia) as a temporary table to the target catalog and run cross matching query
            job = simbad_tap.submit_job(crossmatch_query, uploads={"tmp_table": upload_table})
            job.run()
            job.wait()

            if job.phase == "COMPLETED":
                # Fetch the results
                crossmatch_result = job.fetch_result().to_table()

                # Merge the tables using source_id
                table[catalog_source.ColumnId].description = "Unique source identifier"
                crossmatch_result[catalog_source.ColumnId].description = "Unique source identifier"
                merged_table = join(table, crossmatch_result, keys=[f"{catalog_source.ColumnId}"], join_type="left")

                if len(merged_table) == 0:
                    raise CrossMatchRequestProcessingError(catalog_source.Catalog, query, catalog_to_match.Catalog, crossmatch_query, job.phase, "Cross matching result is empty")

                simbad_columns = ['simbad_id', 'simbad_name', 'simbad_otype', 'simbad_type_description']

                #reorder columns, put simbad id, name ... right after gaia_id
                #the first column is gaia_id
                reordered_columns = [catalog_source.ColumnId]
                #the next columns are simbad id, name, type and type description
                reordered_columns += simbad_columns

                for col in merged_table.colnames:
                    if col not in reordered_columns:
                        reordered_columns.append(col)

                merged_table = merged_table[reordered_columns]

                columns = merged_table.colnames
                rows = [dict(zip(columns, row)) for row in merged_table]

                return catalog_source.Catalog, query, catalog_to_match.Catalog, crossmatch_query, columns, rows
            else:
                raise CrossMatchRequestProcessingError(catalog_source.Catalog, query, catalog_to_match.Catalog, crossmatch_query, job.phase, job.results)

        except Exception as e:
            raise CrossMatchRequestProcessingError(catalog_source.Catalog, query, catalog_to_match.Catalog, crossmatch_query, "Cross match request failed", str(e))

    # set unit degree for the column if it is not specified
    def __setColumnUnitToDeg(self, column):
        if hasattr(column, "unit"):
            column = column.to(u.deg, equivalencies=u.dimensionless_angles())
        else:
            column = column * u.deg
        return column

    #set unit milliarcseconds per year for the column if it is not specified
    def __setColumnUnitToMilliarcsecPerYear(self, column):
        if hasattr(column, "unit"):
            column = column.to(u.mas / u.yr)  # Ensure correct unit
        else:
            column = column * u.mas / u.yr
        return column
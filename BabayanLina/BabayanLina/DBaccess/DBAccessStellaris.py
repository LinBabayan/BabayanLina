from .DBAccessBase import DBAccessBase
from .DBAccessEnums import Category, ObjectTypes

class DBAccessStellaris(DBAccessBase):
    def QueryCatalog(self, form_data, limit, chunk_size):
        return {"error": "Error querying Gaia catalog."}

    @property
    def CategoryInfo(self):
        return {
            Category.Parallax: ('parallax', 'gs'),
            Category.ProperMotion: ('pm', 'gs'),
            Category.RA: ('ra', 'gs'),
            Category.Dec: ('dec', 'gs'),
            Category.GMagnitude: ('gs.phot_g_mean_mag', 'gs'),
            Category.BPMagnitude: ('phot_bp_mean_mag', 'gs'),
            Category.RPMagnitude: ('phot_rp_mean_mag', 'gs'),
            Category.RadialVelocity: ('radial_velocity', 'gs'),
            Category.Mass: ('mass_flame', 'obj'),
            Category.Radius: ('radius_flame', 'obj'),
            Category.Age: (''),
            Category.Luminosity: ('lum_flame', 'obj'),
            Category.Temperature: ('teff_gspphot', 'obj'),
            Category.Gravity: ('logg_gspphot', 'obj')
        }


    @property
    def Catalog(self):
        return "stellaris"

    @property
    def ColumnId(self):
        return "stellaris_id"

#Alinai sql/adql code-ic insert/update-i proceduranery kanchel vor useri queryi ardzyunqum stacac datan lcvi mer database-i mej
from enum import Enum

class Category(Enum):
    # Measured in milliarcseconds (mas).
    Parallax = 'Parallax'
    # Measured in milliarcseconds per year (mas/yr).
    ProperMotion = 'Proper Motion'
    # Position (RA and Dec): Measured in degrees.
    RA = 'RA'
    Dec = 'Dec'
    # Proper motion in RA and Dec (mas/yr)
    PMRA = 'Proper Motion RA'
    PMDec = 'Proper Motion Dec'
    # G-band magnitude.
    GMagnitude = 'G-Magnitude'
    # Blue Photometer: Measured in magnitudes.
    BPMagnitude = 'BP-Magnitude'
    # Red Photometer: Measured in magnitudes.
    RPMagnitude = 'RP-Magnitude'
    # The radial velocity of the star, measured in kilometers per second (km/s).
    RadialVelocity = 'Radial Velocity'
    # Estimated in solar radii (R☉).
    Mass = 'Mass'
    # Estimated in solar radii (R☉).
    Radius = 'Radius'
    Luminosity = 'Luminosity'
    Temperature = 'Temperature'
    Gravity = 'Gravity'
    ObjectsInCircle = 'Objects Inside CIRCLE'

class ObjectTypes(Enum):
    Star = 'Star'
    Galaxy = 'Galaxy'
    Quasar = 'Quasar'
    Undefined = ''
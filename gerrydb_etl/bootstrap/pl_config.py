"""Global configuration and constants for Census PL 94-171 data imports."""
from collections import namedtuple

CENTRAL_SPINE_LEVELS = (
    "block",
    "bg",
    "tract",
    "county",
    "state",
)
# Levels auxiliary to central spine.
AUXILIARY_LEVELS = (
    "vtd",
    "place",
    "cousub",
    "aiannh",  # American Indian/Alaska Native/Native Hawaiian Areas
)
LEVELS = CENTRAL_SPINE_LEVELS + AUXILIARY_LEVELS

# These aren't part of the core PL 94-171 release,
# but we can still get data for them on most units.
OUTLYING_TERRITORIES = (
    "60",  # American Samoa
    "66",  # Guam
    "69",  # Northern Mariana Islands
    "78",  # U.S. Virgin Islands
)

MissingDataset = namedtuple("MissingDataset", ["fips", "level", "year"])

# A few states don't participate or didn't historically participate
# in VTD releases. Outlying territories typically don't have Census VTDs either.
MISSING_DATASETS = (
    # California 2020 VTDs.
    # Not available from the Census. Statewide Database (CA only) doesn't seem
    # to have them, and Redistricting Data Hub's publication is spurious
    # (each of the 58 VTDs have the name "Voting Districts not defined").
    MissingDataset("06", "vtd", "2020"),
    # California 2020 VTDs.
    # Not available from the Census.
    # Redistricting Data Hub's publication is spurious
    # (each of the VTDs have the name "Voting Districts not defined").
    MissingDataset("15", "vtd", "2020"),
    # Kentucky 2010 VTDs.
    # Not available from the Census.
    # Redistricting Data Hub's publication is spurious
    # (each of the VTDs have the name "Voting Districts not defined").
    MissingDataset("21", "vtd", "2010"),
    # Oregon 2020 VTDs.
    # Not available from the Census.
    # Redistricting Data Hub's publication is spurious
    # (each of the VTDs have the name "Voting Districts not defined").
    MissingDataset("41", "vtd", "2020"),
    # Rhode Island VTDs.
    # Not available from the Census.
    # Redistricting Data Hub's publication is spurious
    # (each of the VTDs have the name "Voting Districts not defined").
    MissingDataset("44", "vtd", "2010"),
    # Outlying territories.
    MissingDataset("60", "vtd", "2010"),  # American Samoa
    MissingDataset("60", "vtd", "2020"),
    MissingDataset("66", "vtd", "2010"),  # Guam
    MissingDataset("66", "vtd", "2020"),
    MissingDataset("69", "vtd", "2010"),  # Northern Mariana Islands
    MissingDataset("69", "vtd", "2020"),
    MissingDataset("78", "vtd", "2010"),  # U.S. Virgin Islands
    MissingDataset("78", "vtd", "2020"),
)

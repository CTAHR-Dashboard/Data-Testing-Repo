#*****************************************************************
#
#  PROJECT:     Hawaii Fisheries Dashboard
#
#  CLASS:       CTAHR Data Pipeline
#
#  FILE:        config.py
#
#  DESCRIPTION: Central configuration file for the fisheries data cleaning pipeline.
#               Contains all configurable parameters including file paths, validation
#               rules, expected values, column schemas, and logging settings.
#
#*****************************************************************

from pathlib import Path

class Config:

    BASE_DIR = Path(__file__).parent

    DATA_RAW_DIR = BASE_DIR / 'data' / 'raw'
    DATA_CLEANED_DIR = BASE_DIR / 'data' / 'cleaned'
    LOGS_DIR = BASE_DIR / 'logs'

    COMMERCIAL_FILE_PATTERN = '*tidied_comm_ev*.csv'
    NONCOMMERCIAL_FILE_PATTERN = '*tidied_noncomm_ev*.csv'

    VALID_COUNTIES = [ 'Hawaii', 'Maui', 'Honolulu', 'Kauai', 'Kalawao' ]

    VALID_ISLANDS = [ 'Hawaii', 'Kauai', 'Lanai', 'Maui', 'Molokai', 'Oahu' ]

    COMMERCIAL_MIN_YEAR = 1997
    COMMERCIAL_MAX_YEAR = 2021

    NONCOMMERCIAL_MIN_YEAR = 2005
    NONCOMMERCIAL_MAX_YEAR = 2022

    COMMERCIAL_SPECIES_GROUPS = [ 'Deep 7 Bottomfish', 'Shallow Bottomfish', 'Pelagics', 'Reef-Associated', 'All Species' ]

    NONCOMMERCIAL_SPECIES_GROUPS = ['Herbivores']

    ECOSYSTEM_TYPES = [ 'Inshore — Reef', 'Coastal — Open Ocean', 'All Ecosystems' ]

    AGGREGATE_SPECIES_VALUES = ['All Species']
    AGGREGATE_ECOSYSTEM_VALUES = ['All Ecosystems']

    DISPLAY_ONLY_COLUMNS = [ 'county_olelo', 'island_olelo', 'exchange_value_formatted' ]

    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    EXPORT_TIMESTAMP_FORMAT = '%Y%m%d'

    REQUIRED_COMMERCIAL_COLUMNS = [ 'year', 'area_id', 'county', 'species_group', 'ecosystem_type', 'exchange_value' ]

    REQUIRED_NONCOMMERCIAL_COLUMNS = [ 'year', 'island', 'county', 'species_group', 'ecosystem_type', 'exchange_value']


#*****************************************************************
#
#  Function name: getConfig
#
#  DESCRIPTION:   Factory function that returns a Config instance for the pipeline.
#                 Other modules call this instead of instantiating Config directly
#                 to make it easier to swap configurations for testing later.
#
#  Parameters:    None
#
#  Return values: Config : configuration object
#
#*****************************************************************

def getConfig():
    return Config()

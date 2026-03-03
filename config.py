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

    # DAR statistical area to island mapping
    # Source: DLNR DAR Commercial Marine Landings Report
    # Hawaii county (100 series)
    # Honolulu county (400 series) -> Oahu
    # Kauai county (500 series)
    # Maui county (300 series) split across Maui, Molokai, Lanai, Kahoolawe, and channels

    AREA_ID_TO_ISLAND = {
        # Hawaii county -> Hawaii island
        **{aid: 'Hawaii' for aid in [100, 101, 102, 103, 104, 105, 106, 107, 108,
                                     120, 121, 122, 123, 124, 125, 126, 127, 128]},
        # Maui county -> Maui island (coastal areas and channels)
        **{aid: 'Maui' for aid in [300, 301, 302, 303, 304, 305,
                                   306, 307,           # Kahoolawe (uninhabited, grouped with Maui)
                                   320,                 # Alalakeiki Channel
                                   321,                 # Auau, Kalohi and Pailolo Channels
                                   322, 323,
                                   324, 325,            # Alenuihaha Channel
                                   326]},               # Kahoolawe offshore
        # Maui county -> Lanai island
        **{aid: 'Lanai' for aid in [308, 309,
                                    327,                # Kealaikahiki Channel
                                    328]},
        # Maui county -> Molokai island
        **{aid: 'Molokai' for aid in [310, 311, 312, 313, 314,
                                      331,              # Penguin Bank
                                      332,              # Kaiwi Channel
                                      333]},
        # Honolulu county -> Oahu island
        **{aid: 'Oahu' for aid in [400, 401, 402, 403, 404, 405, 406, 407, 408, 409,
                                   420, 421, 422, 423, 424, 425, 426, 427, 428, 429]},
        # Kauai county -> Kauai island
        **{aid: 'Kauai' for aid in [500, 501, 502, 503, 504, 505, 506, 508,
                                    520, 521, 522, 523, 524, 525, 526, 527, 528]},
    }

    ISLAND_OLELO = {
        'Hawaii': 'Hawaiʻi',
        'Kauai': 'Kauaʻi',
        'Lanai': 'Lānaʻi',
        'Maui': 'Maui',
        'Molokai': 'Molokaʻi',
        'Oahu': 'Oʻahu',
    }

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

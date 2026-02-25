#*****************************************************************
#
#  PROJECT:     Hawaii Fisheries Dashboard
#
#  CLASS:       CTAHR Data Pipeline
#
#  FILE:        clean_noncommercial.py
#
#  DESCRIPTION: Non commercial fisheries data cleaning module.
#               Validates and optionally filters non commercial fisheries tidied data.
#               Since input is pre-tidied, focuses on validation and optional transformations.
#
#*****************************************************************

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime

class NonCommercialDataCleaner:

#*****************************************************************
#
#  Function name: __init__
#
#  DESCRIPTION:   Initializes the NonCommercialDataCleaner by setting input/output
#                 directory paths and ensuring the output directory exists.
#                 Also initializes internal dataset storage and row counters for
#                 auditing cleaning impacts.
#
#  Parameters:    input_dir (str) : path to directory containing the pre-tidied
#                     non commercial CSV file 
#                 output_dir (str) : path to directory where the cleaned CSV
#                     will be saved 
#
#  Return values: None (constructor)
#
#*****************************************************************

    def __init__(self, input_dir='data/raw', output_dir='data/cleaned'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data = None
        self.raw_row_count = 0
        self.cleaned_row_count = 0


#*****************************************************************
#
#  Function name: loadData
#
#  DESCRIPTION:   Locates a non commercial fisheries CSV in the input directory
#                 using expected filename patterns with a broader fallback.
#                 Loads the first match into a DataFrame and records the raw row
#                 count for later comparison.
#
#  Parameters:    None
#
#  Return values: True  : data loaded successfully into self.data
#                 False : no matching file found or a read error occurred
#
#*****************************************************************

    def loadData(self):
        logging.info('Loading non commercial fisheries data ')

        try:
            noncomm_files = list(self.input_dir.glob('*tidied_noncomm_ev*.csv'))

            if not noncomm_files:
                noncomm_files = list(self.input_dir.glob('*noncomm_ev*.csv'))

            if not noncomm_files:
                logging.error(f'No non commercial data file found in {self.input_dir}')
                return False

            self.data = pd.read_csv(noncomm_files[0])
            self.raw_row_count = len(self.data)

            logging.info(f'Loaded {self.raw_row_count:,} rows from {noncomm_files[0].name}')
            return True

        except Exception as e:
            logging.error(f'Error loading non commercial data: {e}')
            return False


#*****************************************************************
#
#  Function name: validateSchema
#
#  DESCRIPTION:   Confirms that required columns exist including island which replaces
#                 area_id from the commercial schema. Also checks for optional display
#                 columns and logs which are present for transparency.
#
#  Parameters:    None
#
#  Return values: True  : all required columns are present
#                 False : one or more required columns missing
#
#*****************************************************************

    def validateSchema(self):
        logging.info('Validating data schema ')

        required_columns = [ 'year', 'island', 'county', 'species_group', 'ecosystem_type', 'exchange_value']

        optional_columns = ['island_olelo', 'county_olelo', 'exchange_value_formatted']

        missing_required = [col for col in required_columns if col not in self.data.columns]

        if missing_required:
            logging.error(f'Missing required columns: {missing_required}')
            return False

        present_optional = [col for col in optional_columns if col in self.data.columns]
        if present_optional:
            logging.info(f'Optional columns present: {present_optional}')

        logging.info('Schema validation passed')
        return True


#*****************************************************************
#
#  Function name: validateDataTypes
#
#  DESCRIPTION:   Converts year to nullable integer and exchange_value to numeric
#                 using coercion to avoid hard failures on bad cells. Logs warnings
#                 when conversions produce nulls indicating data needing review.
#
#  Parameters:    None
#
#  Return values: None 
#
#*****************************************************************

    def validateDataTypes(self):
        logging.info('Validating data types ')

        self.data['year'] = pd.to_numeric(self.data['year'], errors='coerce').astype('Int64')
        self.data['exchange_value'] = pd.to_numeric(self.data['exchange_value'], errors='coerce')

        null_years = self.data['year'].isnull().sum()
        null_values = self.data['exchange_value'].isnull().sum()

        if null_years > 0:

            logging.warning(f'Found {null_years} null years after conversion')
        if null_values > 0:
            logging.warning(f'Found {null_values} null exchange values after conversion')


#*****************************************************************
#
#  Function name: validateDataRanges
#
#  DESCRIPTION:   Checks for negative exchange values and years outside the expected
#                 2005-2022 non commercial range. Logs warnings for out of range values
#                 but does not remove them to avoid silent data loss.
#
#  Parameters:    None
#
#  Return values: True : always returns True (warnings only)
#
#*****************************************************************

    def validateDataRanges(self):
        logging.info('Validating data ranges ')

        issues = []

        if (self.data['exchange_value'] < 0).any():
            negative_count = (self.data['exchange_value'] < 0).sum()
            issues.append(f'{negative_count} negative exchange values')

        if (self.data['year'] < 2005).any() or (self.data['year'] > 2022).any():
            invalid_years = self.data[
                (self.data['year'] < 2005) | (self.data['year'] > 2022)
            ]['year'].unique()
            issues.append(f'Years outside expected range (2005-2022): {invalid_years}')

        if issues:
            for issue in issues:
                logging.warning(f'Data quality issue: {issue}')
        else:
            logging.info('Data range validation passed')

        return True


#*****************************************************************
#
#  Function name: validateEcosystemTypes
#
#  DESCRIPTION:   Verifies ecosystem_type values match the expected set from the
#                 tidying step. All herbivores map to 100% reef habitat so Open
#                 Ocean values should be zero but the category may still appear.
#
#  Parameters:    None
#
#  Return values: None
#
#*****************************************************************

    def validateEcosystemTypes(self):
        logging.info('Validating ecosystem types ')

        expected_ecosystems = [ 'Inshore — Reef', 'Coastal — Open Ocean', 'All Ecosystems']

        unique_ecosystems = self.data['ecosystem_type'].unique()
        unexpected = [e for e in unique_ecosystems if e not in expected_ecosystems]

        if unexpected:
            logging.warning(f'Unexpected ecosystem types: {unexpected}')
        else:
            logging.info(f'All ecosystem types valid: {sorted(unique_ecosystems)}')


#*****************************************************************
#
#  Function name: validateSpeciesGroups
#
#  DESCRIPTION:   Validates that non commercial data only contains the Herbivores
#                 species group unlike commercial which has four groups plus aggregate.
#                 Logs any unexpected groups to flag upstream data issues.
#
#  Parameters:    None
#
#  Return values: None
#
#*****************************************************************

    def validateSpeciesGroups(self):
        logging.info('Validating species groups ')

        expected_species = ['Herbivores']

        unique_species = self.data['species_group'].unique()
        unexpected = [s for s in unique_species if s not in expected_species]

        if unexpected:
            logging.warning(f'Unexpected species groups: {unexpected}')
        else:
            logging.info(f'Species group validation passed: {sorted(unique_species)}')


#*****************************************************************
#
#  Function name: validateIslands
#
#  DESCRIPTION:   Checks island names against the six Hawaiian islands covered by
#                 the MRIP survey. Niihau and Kahoolawe are excluded because the
#                 MRIP survey does not sample those islands.
#
#  Parameters:    None
#
#  Return values: None
#
#*****************************************************************

    def validateIslands(self):
        logging.info('Validating island names ')

        expected_islands = [ 'Hawaii', 'Kauai', 'Lanai', 'Maui', 'Molokai', 'Oahu']

        unique_islands = self.data['island'].unique()
        unexpected = [i for i in unique_islands if i not in expected_islands]

        if unexpected:
            logging.warning(f'Unexpected island names: {unexpected}')
        else:
            logging.info(f'All islands valid: {sorted(unique_islands)}')


#*****************************************************************
#
#  Function name: validateCounties
#
#  DESCRIPTION:   Validates county names against the four counties present in non
#                 commercial data. Kalawao is absent because Molokai maps to Maui
#                 county in the MRIP survey design.
#
#  Parameters:    None
#
#  Return values: None
#
#*****************************************************************

    def validateCounties(self):
        logging.info('Validating county names ')

        expected_counties = ['Hawaii', 'Maui', 'Honolulu', 'Kauai']

        unique_counties = self.data['county'].unique()
        unexpected = [c for c in unique_counties if c not in expected_counties]

        if unexpected:
            logging.warning(f'Unexpected county names: {unexpected}')
        else:
            logging.info(f'All counties valid: {sorted(unique_counties)}')


#*****************************************************************
#
#  Function name: removeNullValues
#
#  DESCRIPTION:   Removes rows where exchange_value is null/NaN because they cannot be
#                 aggregated or visualized reliably. Logs the number removed to make
#                 the cleaning impact transparent in pipeline outputs.
#
#  Parameters:    None
#
#  Return values: None 
#
#*****************************************************************

    def removeNullValues(self):
        logging.info('Removing null/NA exchange values ')

        before_count = len(self.data)
        self.data = self.data[self.data['exchange_value'].notna()].copy()
        after_count = len(self.data)

        removed = before_count - after_count
        if removed > 0:
            logging.info(f'Removed {removed:,} records with null/NA exchange values')
        else:
            logging.info('No null/NA values to remove')


#*****************************************************************
#
#  Function name: removeAggregateRows
#
#  DESCRIPTION:   Optionally removes All Ecosystems rollup rows to prevent double
#                 counting during totals and aggregations. No All Species filter is
#                 needed since noncommercial only contains the Herbivores group.
#
#  Parameters:    remove_aggregates (bool) : if True, remove aggregate rows (default: True)
#
#  Return values: None 
#
#*****************************************************************

    def removeAggregateRows(self, remove_aggregates=True):
        if not remove_aggregates:
            logging.info('Skipping aggregate row removal (remove_aggregates=False)')
            return

        logging.info('Removing aggregate rows ')

        before_count = len(self.data)

        self.data = self.data[
            ~self.data['ecosystem_type'].isin(['All Ecosystems'])
        ].copy()

        after_count = len(self.data)
        removed = before_count - after_count

        if removed > 0:
            logging.info(f'Removed {removed:,} aggregate rows')
        else:
            logging.info('No aggregate rows found to remove')


#*****************************************************************
#
#  Function name: removeDisplayColumns
#
#  DESCRIPTION:   Optionally drops display only columns including island_olelo,
#                 county_olelo, and exchange_value_formatted. This reduces file size
#                 and prevents accidental use of formatted strings in numeric workflows.
#
#  Parameters:    remove_display (bool) : if True, drop display columns (default: False)
#
#  Return values: None 
#
#*****************************************************************

    def removeDisplayColumns(self, remove_display=False):
        if not remove_display:
            logging.info('Keeping display columns')
            return

        logging.info('Removing display only columns ')

        display_columns = ['island_olelo', 'county_olelo', 'exchange_value_formatted']
        columns_to_drop = [col for col in display_columns if col in self.data.columns]

        if columns_to_drop:
            self.data = self.data.drop(columns=columns_to_drop)
            logging.info(f'Removed columns: {columns_to_drop}')
        else:
            logging.info('No display columns to remove')


#*****************************************************************
#
#  Function name: generateSummaryStatistics
#
#  DESCRIPTION:   Generates a structured summary of the cleaned dataset for auditing
#                 and dashboard consumption. Includes row counts, year range, totals,
#                 unique category values, and per year/per island breakdowns.
#
#  Parameters:    None
#
#  Return values: dict : summary statistics dictionary
#
#*****************************************************************

    def generateSummaryStatistics(self):
        logging.info('Generating summary statistics ')

        summary = {
            'data_type': 'non_commercial',
            'processing_timestamp': datetime.now().isoformat(),
            'raw_row_count': self.raw_row_count,
            'cleaned_row_count': len(self.data),
            'rows_removed': self.raw_row_count - len(self.data),
            'date_range': {
                'min_year': int(self.data['year'].min()),
                'max_year': int(self.data['year'].max())
            },
            'total_exchange_value': float(self.data['exchange_value'].sum()),
            'unique_islands': sorted(self.data['island'].unique().tolist()),
            'unique_counties': sorted(self.data['county'].unique().tolist()),
            'unique_species_groups': sorted(self.data['species_group'].unique().tolist()),
            'unique_ecosystem_types': sorted(self.data['ecosystem_type'].unique().tolist()),
            'records_by_year': self.data.groupby('year').size().to_dict(),
            'records_by_island': self.data.groupby('island').size().to_dict(),
            'total_value_by_year': self.data.groupby('year')['exchange_value'].sum().to_dict()
        }

        return summary


#*****************************************************************
#
#  Function name: exportCleanedData
#
#  DESCRIPTION:   Exports the cleaned dataset to a timestamped CSV in the output directory
#                 so each pipeline run produces a unique artifact. Updates the cleaned row
#                 counter and returns the full output file path for downstream use.
#
#  Parameters:    None
#
#  Return values: Path : path to exported cleaned CSV
#
#*****************************************************************

    def exportCleanedData(self):
        logging.info('Exporting cleaned non commercial data ')

        timestamp = datetime.now().strftime('%Y%m%d')
        output_file = self.output_dir / f'cleaned_noncommercial_{timestamp}.csv'

        self.data.to_csv(output_file, index=False)
        self.cleaned_row_count = len(self.data)

        logging.info(f'Exported {self.cleaned_row_count:,} rows to {output_file}')
        return output_file


#*****************************************************************
#
#  Function name: runCleaningPipeline
#
#  DESCRIPTION:   Runs the full non commercial cleaning workflow including island
#                 validation and stops early if load or schema validation fails.
#                 Applies all validators, null removal, optional cleanup, then exports.
#
#  Parameters:    remove_aggregates (bool) : remove All Ecosystems rows (default: True)
#                 remove_display (bool) : remove display only columns (default: False)
#
#  Return values: tuple (bool, Path, dict) : (success, exported_csv_path, summary_stats)
#
#*****************************************************************

    def runCleaningPipeline(self, remove_aggregates=True, remove_display=False):

        logging.info('NON COMMERCIAL FISHERIES DATA CLEANING PIPELINE')

        if not self.loadData():
            return False, None, None

        if not self.validateSchema():
            return False, None, None

        self.validateDataTypes()
        self.validateDataRanges()
        self.validateEcosystemTypes()
        self.validateSpeciesGroups()
        self.validateIslands()
        self.validateCounties()

        self.removeNullValues()

        self.removeAggregateRows(remove_aggregates=remove_aggregates)
        self.removeDisplayColumns(remove_display=remove_display)

        output_file = self.exportCleanedData()
        summary = self.generateSummaryStatistics()

        logging.info('NON COMMERCIAL DATA CLEANING COMPLETE')
        logging.info(f'Input:  {self.raw_row_count:,} rows')
        logging.info(f'Output: {self.cleaned_row_count:,} rows')
        logging.info(f'Removed: {self.raw_row_count - self.cleaned_row_count:,} rows')

        return True, output_file, summary

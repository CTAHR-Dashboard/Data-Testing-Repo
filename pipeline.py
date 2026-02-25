#*****************************************************************
#
#  PROJECT:     Hawaii Fisheries Dashboard
#
#  CLASS:       CTAHR Data Pipeline
#
#  FILE:        pipeline.py
#
#  DESCRIPTION: Main orchestrator for the fisheries data cleaning pipeline.
#               Coordinates cleaning of both commercial and non commercial datasets
#               then generates combined summaries and triggers dashboard generation.
#
#*****************************************************************

import json
import logging
from pathlib import Path
from datetime import datetime
from clean_commercial import CommercialDataCleaner
from clean_noncommercial import NonCommercialDataCleaner

class FisheriesCleaningPipeline:

#*****************************************************************
#
#  Function name: __init__
#
#  DESCRIPTION:   Initializes the pipeline by setting up directories and instantiating
#                 both the CommercialDataCleaner and NonCommercialDataCleaner. Also
#                 creates a results dictionary that stores each sub pipeline outcome.
#
#  Parameters:    input_dir (str) : path to raw CSV directory 
#                 output_dir (str) : path to cleaned output directory
#
#  Return values: None (constructor)
#
#*****************************************************************

    def __init__(self, input_dir='data/raw', output_dir='data/cleaned'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.commercial_cleaner = CommercialDataCleaner(input_dir, output_dir)
        self.noncommercial_cleaner = NonCommercialDataCleaner(input_dir, output_dir)

        self.results = {
            'commercial': None,
            'non_commercial': None
        }

#*****************************************************************
#
#  Function name: setupLogging
#
#  DESCRIPTION:   Configures Python logging with both a timestamped log file and
#                 console output handler. Creates the logs directory if it does not
#                 exist so the pipeline can run from a fresh checkout.
#
#  Parameters:    None
#
#  Return values: None
#
#*****************************************************************

    def setupLogging(self):
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f'cleaning_pipeline_{timestamp}.log'

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        logging.info(f'Logging initialized: {log_file}')


#*****************************************************************
#
#  Function name: runCommercialCleaning
#
#  DESCRIPTION:   Executes the commercial data cleaning sub pipeline and stores
#                 the results including success status, output path, and summary
#                 statistics for use in the final report.
#
#  Parameters:    remove_aggregates (bool) : remove rollup rows 
#                 remove_display (bool) : drop display columns 
#
#  Return values: True  : cleaning succeeded
#                 False : cleaning failed
#
#*****************************************************************

    def runCommercialCleaning(self, remove_aggregates=True, remove_display=False):
        logging.info('STARTING COMMERCIAL DATA CLEANING')

        success, output_file, summary = self.commercial_cleaner.runCleaningPipeline(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )

        if success:
            self.results['commercial'] = {
                'success': True,
                'output_file': str(output_file),
                'summary': summary
            }
            return True
        else:
            self.results['commercial'] = {
                'success': False,
                'error': 'Commercial data cleaning failed'
            }
            return False

#*****************************************************************
#
#  Function name: runNoncommercialCleaning
#
#  DESCRIPTION:   Executes the non commercial data cleaning sub pipeline and stores
#                 the results including success status, output path, and summary
#                 statistics for use in the final report.
#
#  Parameters:    remove_aggregates (bool) : remove rollup rows 
#                 remove_display (bool) : drop display columns 
#
#  Return values: True  : cleaning succeeded
#                 False : cleaning failed
#
#*****************************************************************

    def runNoncommercialCleaning(self, remove_aggregates=True, remove_display=False):
        logging.info('STARTING NON COMMERCIAL DATA CLEANING')

        success, output_file, summary = self.noncommercial_cleaner.runCleaningPipeline(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )

        if success:
            self.results['non_commercial'] = {
                'success': True,
                'output_file': str(output_file),
                'summary': summary
            }
            return True
        else:
            self.results['non_commercial'] = {
                'success': False,
                'error': 'Non commercial data cleaning failed'
            }
            return False


#*****************************************************************
#
#  Function name: generateCombinedSummary
#
#  DESCRIPTION:   Merges summary statistics from both cleaners into one dictionary
#                 and computes combined totals if both datasets succeeded. Returns
#                 the combined summary for JSON export and downstream consumption.
#
#  Parameters:    None
#
#  Return values: dict : combined summary statistics
#
#*****************************************************************

    def generateCombinedSummary(self):
        logging.info('Generating combined summary statistics...')

        combined_summary = {
            'pipeline_timestamp': datetime.now().isoformat(),
            'commercial': self.results['commercial']['summary'] if self.results['commercial'] else None,
            'non_commercial': self.results['non_commercial']['summary'] if self.results['non_commercial'] else None
        }

        if self.results['commercial'] and self.results['non_commercial']:
            combined_summary['overall'] = {
                'total_records': (
                    self.results['commercial']['summary']['cleaned_row_count'] +
                    self.results['non_commercial']['summary']['cleaned_row_count']
                ),
                'total_exchange_value': (
                    self.results['commercial']['summary']['total_exchange_value'] +
                    self.results['non_commercial']['summary']['total_exchange_value']
                ),
                'combined_date_range': {
                    'min_year': min(
                        self.results['commercial']['summary']['date_range']['min_year'],
                        self.results['non_commercial']['summary']['date_range']['min_year']
                    ),
                    'max_year': max(
                        self.results['commercial']['summary']['date_range']['max_year'],
                        self.results['non_commercial']['summary']['date_range']['max_year']
                    )
                }
            }

        return combined_summary

#*****************************************************************
#
#  Function name: exportSummaryJson
#
#  DESCRIPTION:   Writes the combined summary dictionary to a timestamped JSON file
#                 in the output directory for downstream applications. Returns the
#                 full path so callers can reference or log the output location.
#
#  Parameters:    summary (dict) : combined summary from generateCombinedSummary
#
#  Return values: Path : path to the exported JSON file
#
#*****************************************************************

    def exportSummaryJson(self, summary):
        logging.info('Exporting summary statistics to JSON...')

        timestamp = datetime.now().strftime('%Y%m%d')
        output_file = self.output_dir / f'cleaning_summary_{timestamp}.json'

        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)

        logging.info(f'Summary exported to {output_file}')
        return output_file

#*****************************************************************
#
#  Function name: generatePipelineReport
#
#  DESCRIPTION:   Prints a human readable summary of the pipeline results to both
#                 console and log file. Shows row counts, date ranges, total values,
#                 and unique dimension counts for each dataset.
#
#  Parameters:    None
#
#  Return values: None (prints to log and console)
#
#*****************************************************************

    def generatePipelineReport(self):
        logging.info('')
        logging.info('FISHERIES DATA CLEANING PIPELINE : FINAL REPORT')

        if self.results['commercial'] and self.results['commercial']['success']:
            comm_summary = self.results['commercial']['summary']
            logging.info('')
            logging.info('COMMERCIAL FISHERIES:')
            logging.info(f"  Status: SUCCESS")
            logging.info(f"  Input Rows:  {comm_summary['raw_row_count']:,}")
            logging.info(f"  Output Rows: {comm_summary['cleaned_row_count']:,}")
            logging.info(f"  Removed:     {comm_summary['rows_removed']:,}")
            logging.info(f"  Date Range:  {comm_summary['date_range']['min_year']}-{comm_summary['date_range']['max_year']}")
            logging.info(f"  Total Value: ${comm_summary['total_exchange_value']:,.2f}")
            logging.info(f"  Counties:    {len(comm_summary['unique_counties'])}")
            logging.info(f"  Species:     {len(comm_summary['unique_species_groups'])}")
            logging.info(f"  DAR Areas:   {len(comm_summary['unique_area_ids'])}")
        else:
            logging.info('')
            logging.info('COMMERCIAL FISHERIES: FAILED')

        if self.results['non_commercial'] and self.results['non_commercial']['success']:
            noncomm_summary = self.results['non_commercial']['summary']
            logging.info('')
            logging.info('NON COMMERCIAL FISHERIES:')
            logging.info(f"  Status: SUCCESS")
            logging.info(f"  Input Rows:  {noncomm_summary['raw_row_count']:,}")
            logging.info(f"  Output Rows: {noncomm_summary['cleaned_row_count']:,}")
            logging.info(f"  Removed:     {noncomm_summary['rows_removed']:,}")
            logging.info(f"  Date Range:  {noncomm_summary['date_range']['min_year']}-{noncomm_summary['date_range']['max_year']}")
            logging.info(f"  Total Value: ${noncomm_summary['total_exchange_value']:,.2f}")
            logging.info(f"  Islands:     {len(noncomm_summary['unique_islands'])}")
        else:
            logging.info('')
            logging.info('NON COMMERCIAL FISHERIES: FAILED')

        logging.info('')


#*****************************************************************
#
#  Function name: runFullPipeline
#
#  DESCRIPTION:   Executes the complete end to end pipeline by setting up logging,
#                 running both cleaners independently, and printing the final report.
#                 Both sub pipelines run so one failure does not block the other.
#
#  Parameters:    remove_aggregates (bool) : remove rollup rows 
#                 remove_display (bool) : drop display columns 
#
#  Return values: True  : both datasets cleaned successfully
#                 False : one or both failed
#
#*****************************************************************

    def runFullPipeline(self, remove_aggregates=True, remove_display=False):
        self.setupLogging()

        logging.info('FISHERIES DATA CLEANING PIPELINE - START')
        logging.info(f'Input Directory:  {self.input_dir.absolute()}')
        logging.info(f'Output Directory: {self.output_dir.absolute()}')
        logging.info(f'Remove Aggregates: {remove_aggregates}')
        logging.info(f'Remove Display Columns: {remove_display}')

        comm_success = self.runCommercialCleaning(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )

        noncomm_success = self.runNoncommercialCleaning(
            remove_aggregates=remove_aggregates,
            remove_display=remove_display
        )

        self.generatePipelineReport()

        overall_success = comm_success and noncomm_success

        if overall_success:
            logging.info('Pipeline Status: SUCCESS')
        else:
            logging.info('Pipeline Status: PARTIAL SUCCESS OR FAILURE')

        return overall_success


#*****************************************************************
#
#  Function name: main
#
#  DESCRIPTION:   Entry point that creates the pipeline instance and runs the full
#                 cleaning process with default settings. Also triggers the dashboard
#                 HTML generation after cleaning completes.
#
#  Parameters:    None
#
#  Return values: True  : pipeline completed successfully
#                 False : pipeline encountered errors
#
#*****************************************************************

def main():
    pipeline = FisheriesCleaningPipeline(
        input_dir='data/raw',
        output_dir='data/cleaned'
    )

    success = pipeline.runFullPipeline(
        remove_aggregates=False,
        remove_display=False
    )

    if success:
        print(" Data cleaning completed successfully*")
        print(f" Cleaned files saved to: data/cleaned/")
    else:
        print("\n✗ Data cleaning encountered errors. Check logs for details.")

    return success


if __name__ == '__main__':
    main()

from generate_dashboard import DashboardGenerator


DashboardGenerator(data_dir='data/cleaned', output_dir='data/cleaned').generate()

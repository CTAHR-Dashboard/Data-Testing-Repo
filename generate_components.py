#*****************************************************************
#
#  PROJECT:     Hawaii Fisheries Dashboard
#
#  CLASS:       CTAHR Data Pipeline
#
#  FILE:        generate_components.py
#
#  DESCRIPTION: Side panel chart component generator that reads cleaned CSVs and
#               produces a self contained HTML panel embeddable via iframe or JS
#               injection. Uses a template file with placeholder token replacement.
#
#*****************************************************************

import csv
import json
import os
import glob
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ComponentGenerator:

#*****************************************************************
#
#  Function name: __init__
#
#  DESCRIPTION:   Initializes the ComponentGenerator by setting the input data
#                 directory and output directory for the HTML component. Both
#                 default to data/cleaned since components sit alongside cleaned CSVs.
#
#  Parameters:    data_dir (str) : path to cleaned CSV directory (default: 'data/cleaned')
#                 output_dir (str) : path for HTML output (default: 'data/cleaned')
#
#  Return values: None (constructor)
#
#*****************************************************************

    def __init__(self, data_dir='data/cleaned', output_dir='data/cleaned'):
        self.data_dir = data_dir
        self.output_dir = output_dir


#*****************************************************************
#
#  Function name: findLatestCsv
#
#  DESCRIPTION:   Searches for CSV files matching a glob pattern and returns the
#                 most recent by alphabetical filename sort. Since files are timestamped
#                 like cleaned_commercial_20260223.csv, sorting gives the latest.
#
#  Parameters:    pattern (str) : glob pattern to match
#
#  Return values: str  : path to latest matching CSV
#                 None : no files matched
#
#*****************************************************************

    def findLatestCsv(self, pattern):
        matches = sorted(glob.glob(os.path.join(self.data_dir, pattern)))
        return matches[-1] if matches else None


#*****************************************************************
#
#  Function name: loadCsv
#
#  DESCRIPTION:   Opens a CSV file and reads it into a list of dictionaries using
#                 csv.DictReader. Each dictionary represents one row with column
#                 headers as keys and cell values as strings.
#
#  Parameters:    filepath (str) : full path to the CSV file
#
#  Return values: list : list of row dictionaries
#
#*****************************************************************

    def loadCsv(self, filepath):
        rows = []
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        return rows


#*****************************************************************
#
#  Function name: normalizeCommercial
#
#  DESCRIPTION:   Converts raw commercial CSV rows to a common schema with proper
#                 types and tags each row as commercial. Sets island to empty string
#                 since commercial data uses area_id/county instead.
#
#  Parameters:    rows (list) : list of dicts from loadCsv
#
#  Return values: list : normalized dicts with proper types
#
#*****************************************************************

    def normalizeCommercial(self, rows):
        out = []
        for r in rows:
            try:
                out.append({
                    'type': 'commercial',
                    'year': int(r.get('year', 0)),
                    'county': r.get('county', ''),
                    'island': '',
                    'species_group': r.get('species_group', ''),
                    'ecosystem_type': r.get('ecosystem_type', ''),
                    'exchange_value': float(r.get('exchange_value', 0)),
                })
            except (ValueError, TypeError):
                continue
        return out


#*****************************************************************
#
#  Function name: normalizeNoncommercial
#
#  DESCRIPTION:   Converts raw non commercial CSV rows to the same normalized schema
#                 as commercial with proper type casting. Includes the island field
#                 which commercial data does not have.
#
#  Parameters:    rows (list) : list of dicts from loadCsv
#
#  Return values: list : normalized dicts with proper types
#
#*****************************************************************

    def normalizeNoncommercial(self, rows):
        out = []
        for r in rows:
            try:
                out.append({
                    'type': 'noncommercial',
                    'year': int(r.get('year', 0)),
                    'county': r.get('county', ''),
                    'island': r.get('island', ''),
                    'species_group': r.get('species_group', ''),
                    'ecosystem_type': r.get('ecosystem_type', ''),
                    'exchange_value': float(r.get('exchange_value', 0)),
                })
            except (ValueError, TypeError):
                continue
        return out


#*****************************************************************
#
#  Function name: generate
#
#  DESCRIPTION:   Main entry point that finds latest CSVs, loads and normalizes
#                 them, injects data into the HTML template, and writes a timestamped
#                 output file to the output directory.
#
#  Parameters:    None
#
#  Return values: str  : path to the generated HTML file
#                 None : no data was available
#
#*****************************************************************

    def generate(self):
        comm_file = self.findLatestCsv('cleaned_commercial_*.csv')
        noncomm_file = self.findLatestCsv('cleaned_noncommercial_*.csv')

        commercial, noncommercial = [], []

        if comm_file:
            logger.info(f"Loading commercial data from {comm_file}")
            commercial = self.normalizeCommercial(self.loadCsv(comm_file))
            logger.info(f"  -> {len(commercial)} commercial rows")
        else:
            logger.warning("No cleaned commercial CSV found")

        if noncomm_file:
            logger.info(f"Loading non commercial data from {noncomm_file}")
            noncommercial = self.normalizeNoncommercial(self.loadCsv(noncomm_file))
            logger.info(f"  -> {len(noncommercial)} non commercial rows")
        else:
            logger.warning("No cleaned non commercial CSV found")

        if not commercial and not noncommercial:
            logger.error("No data - component not generated")
            return None

        all_years = sorted(set(
            [r['year'] for r in commercial] + [r['year'] for r in noncommercial]
        ))
        year_min = all_years[0] if all_years else 1997
        year_max = all_years[-1] if all_years else 2022

        timestamp = datetime.now().strftime('%Y%m%d')
        output_path = os.path.join(self.output_dir, f'chart_component_{timestamp}.html')

        html = self.buildComponent(
            json.dumps(commercial),
            json.dumps(noncommercial),
            year_min, year_max
        )

        os.makedirs(self.output_dir, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

        logger.info(f"Component generated -> {output_path}")
        print(f"  CHART COMPONENT GENERATED")
        print(f"  -> {output_path}")
        print(f"  Embed via <iframe> or open in browser.")
        return output_path


#*****************************************************************
#
#  Function name: buildComponent
#
#  DESCRIPTION:   Reads chart_panel_template.html and replaces placeholder tokens
#                 with actual JSON data and year bounds. Returns a complete self-
#                 contained HTML string ready to be written to disk.
#
#  Parameters:    comm_json (str) : JSON string of commercial data
#                 noncomm_json (str) : JSON string of non commercial data
#                 year_min (int) : earliest year for slider
#                 year_max (int) : latest year for slider
#
#  Return values: str : complete HTML document as a string
#
#*****************************************************************

    def buildComponent(self, comm_json, noncomm_json, year_min, year_max):
        gen_date = datetime.now().strftime('%Y-%m-%d')

        template_path = os.path.join(os.path.dirname(__file__), 'chart_panel_template.html')
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()

        html = html.replace('__COMM_DATA__', comm_json)
        html = html.replace('__NONCOMM_DATA__', noncomm_json)
        html = html.replace('__YEAR_MIN__', str(year_min))
        html = html.replace('__YEAR_MAX__', str(year_max))
        html = html.replace('__GEN_DATE__', gen_date)

        return html


#*****************************************************************
#
#  STANDALONE EXECUTION
#
#  DESCRIPTION:   Runs the component generator independently for testing against
#                 the default data/cleaned directory. Sets up basic logging and
#                 prints an error message if no cleaned CSVs are found.
#
#*****************************************************************

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    gen = ComponentGenerator(
        data_dir='data/cleaned',
        output_dir='data/cleaned'
    )
    result = gen.generate()
    if not result:
        print("ERROR: No data found. Run the cleaning pipeline first.")
        print("  Expected files in data/cleaned/:")
        print("  cleaned_commercial_*.csv")
        print("  cleaned_noncommercial_*.csv")

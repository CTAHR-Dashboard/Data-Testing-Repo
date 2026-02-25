#*****************************************************************
#
#  PROJECT:     Hawaii Fisheries Dashboard
#
#  CLASS:       CTAHR Data Pipeline
#
#  FILE:        generate_dashboard.py
#
#  DESCRIPTION: Full page dashboard generator that reads cleaned CSVs and produces
#               a self-contained Chart.js HTML file with all data embedded as JSON.
#               The generated file works offline with no server required.
#
#*****************************************************************

import csv
import json
import os
import glob
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DashboardGenerator:

#*****************************************************************
#
#  Function name: __init__
#
#  DESCRIPTION:   Initializes the DashboardGenerator by setting the input data
#                 directory and output directory for the HTML file. Both default
#                 to data/cleaned since the dashboard sits alongside cleaned CSVs.
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
        if not matches:
            return None
        return matches[-1]


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
        normalized = []
        for r in rows:
            try:
                normalized.append({
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
        return normalized


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
        normalized = []
        for r in rows:
            try:
                normalized.append({
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
        return normalized


#*****************************************************************
#
#  Function name: computeSummary
#
#  DESCRIPTION:   Computes summary statistics from both datasets for the HTML stat
#                 cards including totals, year ranges, top species, and top county.
#                 Returns a dictionary that gets injected into the template.
#
#  Parameters:    commercial (list) : normalized commercial rows
#                 noncommercial (list) : normalized non commercial rows
#
#  Return values: dict : summary statistics for display
#
#*****************************************************************

    def computeSummary(self, commercial, noncommercial):
        comm_total = sum(r['exchange_value'] for r in commercial)
        noncomm_total = sum(r['exchange_value'] for r in noncommercial)
        comm_years = sorted(set(r['year'] for r in commercial)) if commercial else []
        noncomm_years = sorted(set(r['year'] for r in noncommercial)) if noncommercial else []

        species_totals = {}
        for r in commercial + noncommercial:
            sp = r['species_group']
            species_totals[sp] = species_totals.get(sp, 0) + r['exchange_value']
        top_species = max(species_totals, key=species_totals.get) if species_totals else '—'
        top_species_val = species_totals.get(top_species, 0)

        county_totals = {}
        for r in commercial + noncommercial:
            c = r['county']
            if c:
                county_totals[c] = county_totals.get(c, 0) + r['exchange_value']
        top_county = max(county_totals, key=county_totals.get) if county_totals else '—'
        top_county_val = county_totals.get(top_county, 0)

        return {
            'comm_total': comm_total,
            'noncomm_total': noncomm_total,
            'comm_year_min': comm_years[0] if comm_years else None,
            'comm_year_max': comm_years[-1] if comm_years else None,
            'noncomm_year_min': noncomm_years[0] if noncomm_years else None,
            'noncomm_year_max': noncomm_years[-1] if noncomm_years else None,
            'top_species': top_species,
            'top_species_val': top_species_val,
            'top_county': top_county,
            'top_county_val': top_county_val,
            'total_records': len(commercial) + len(noncommercial),
        }


#*****************************************************************
#
#  Function name: generate
#
#  DESCRIPTION:   Main entry point that finds latest CSVs, loads and normalizes
#                 them, builds the complete HTML dashboard, and writes it to a
#                 timestamped file in the output directory.
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

        commercial = []
        noncommercial = []

        if comm_file:
            logger.info(f"Loading commercial data from {comm_file}")
            raw_comm = self.loadCsv(comm_file)
            commercial = self.normalizeCommercial(raw_comm)
            logger.info(f"  {len(commercial)} commercial rows loaded")
        else:
            logger.warning("No cleaned commercial CSV found — skipping")

        if noncomm_file:
            logger.info(f"Loading non commercial data from {noncomm_file}")
            raw_noncomm = self.loadCsv(noncomm_file)
            noncommercial = self.normalizeNoncommercial(raw_noncomm)
            logger.info(f"  {len(noncommercial)} non commercial rows loaded")
        else:
            logger.warning("No cleaned non commercial CSV found — skipping")

        if not commercial and not noncommercial:
            logger.error("No data available — dashboard not generated")
            return None

        summary = self.computeSummary(commercial, noncommercial)

        timestamp = datetime.now().strftime('%Y%m%d')
        output_filename = f'dashboard_{timestamp}.html'
        output_path = os.path.join(self.output_dir, output_filename)

        html = self.buildHtml(commercial, noncommercial, summary)

        os.makedirs(self.output_dir, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

        logger.info(f"Dashboard generated → {output_path}")
        print(f" Dashboard Generated")
        print(f" File: {output_path}")

        return output_path


#*****************************************************************
#
#  Function name: fmtMoney
#
#  DESCRIPTION:   Formats a dollar value into a compact display string like $3.6M
#                 or $523K for use in the HTML stat cards. Handles billions, millions,
#                 thousands, and smaller values with appropriate precision.
#
#  Parameters:    val (float) : dollar amount to format
#
#  Return values: str : formatted string
#
#*****************************************************************

    def fmtMoney(self, val):
        if val >= 1e9:
            return f'${val/1e9:.2f}B'
        if val >= 1e6:
            return f'${val/1e6:.1f}M'
        if val >= 1e3:
            return f'${val/1e3:.0f}K'
        return f'${val:.0f}'


#*****************************************************************
#
#  Function name: buildHtml
#
#  DESCRIPTION:   Builds the complete self-contained HTML string with embedded CSS,
#                 JavaScript, Chart.js charts, and JSON data. This is the largest
#                 function because it contains the entire dashboard template.
#
#  Parameters:    commercial (list) : normalized commercial data
#                 noncommercial (list) : normalized non commercial data
#                 summary (dict) : precomputed summary stats
#
#  Return values: str : complete HTML document as a string
#
#*****************************************************************

    def buildHtml(self, commercial, noncommercial, summary):
        comm_json = json.dumps(commercial)
        noncomm_json = json.dumps(noncommercial)
        summary_json = json.dumps(summary)

        s = summary
        comm_total_display = self.fmtMoney(s['comm_total'])
        noncomm_total_display = self.fmtMoney(s['noncomm_total'])
        comm_years_display = f"{s['comm_year_min']}–{s['comm_year_max']}" if s['comm_year_min'] else 'No data'
        noncomm_years_display = f"{s['noncomm_year_min']}–{s['noncomm_year_max']}" if s['noncomm_year_min'] else 'No data'
        top_species_val_display = self.fmtMoney(s['top_species_val'])
        top_county_val_display = self.fmtMoney(s['top_county_val'])
        generated_date = datetime.now().strftime('%B %d, %Y at %I:%M %p')

        all_years = sorted(set(
            [r['year'] for r in commercial] + [r['year'] for r in noncommercial]
        ))
        year_min = all_years[0] if all_years else 1997
        year_max = all_years[-1] if all_years else 2022

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hawai&#x02BB;i Fisheries Exchange Value Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=IBM+Plex+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{--deep-ocean:#0B1D3A;--mid-ocean:#133B5C;--reef-teal:#1B998B;--lagoon:#2EC4B6;--sand:#F4E8C1;--coral:#FF6B6B;--sunset:#F77F00;--plumeria:#FFD166;--card-bg:rgba(255,255,255,0.92);--card-border:rgba(27,153,139,0.15);--shadow:0 4px 24px rgba(11,29,58,0.08);--shadow-hover:0 8px 40px rgba(11,29,58,0.14);--radius:16px;--radius-sm:10px;}}
        *{{margin:0;padding:0;box-sizing:border-box;}}

        /* UPDATED: BLACK BACKGROUND + remove header wave look */
        body{{font-family:'IBM Plex Sans',sans-serif;background:#000;color:#fff;min-height:100vh;}}

        /* Main wrapper no longer paints a light background */
        .main-bg{{background:transparent;padding:2.5rem 1.5rem 4rem;}}

        .container{{max-width:1360px;margin:0 auto;}}

        /* Remove the negative lift that was designed for hero/wave */
        .stats-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1rem;margin-top:0;position:relative;z-index:2;margin-bottom:2rem;}}

        /* Dark theme cards */
        :root{{--card-bg:rgba(20,20,20,0.92);--card-border:rgba(255,255,255,0.08);--shadow:0 4px 24px rgba(0,0,0,0.35);--shadow-hover:0 8px 40px rgba(0,0,0,0.55);}}

        .stat-card{{background:var(--card-bg);backdrop-filter:blur(12px);border:1px solid var(--card-border);border-radius:var(--radius);padding:1.3rem 1.5rem;box-shadow:var(--shadow);transition:transform .25s,box-shadow .25s;}}
        .stat-card:hover{{transform:translateY(-3px);box-shadow:var(--shadow-hover);}}
        .stat-label{{font-size:0.65rem;text-transform:uppercase;letter-spacing:1.8px;color:rgba(255,255,255,0.65);font-weight:600;margin-bottom:0.3rem;}}
        .stat-value{{font-family:'DM Serif Display',serif;font-size:1.6rem;color:#fff;line-height:1.2;}}
        .stat-sub{{font-size:0.75rem;color:var(--lagoon);font-weight:500;margin-top:0.15rem;}}

        .controls-panel{{background:var(--card-bg);border:1px solid var(--card-border);border-radius:var(--radius);padding:1.4rem 1.6rem;box-shadow:var(--shadow);margin-bottom:2rem;}}
        .controls-title{{font-weight:600;font-size:0.92rem;color:#fff;margin-bottom:1rem;display:flex;align-items:center;gap:0.5rem;}}
        .controls-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:1rem;align-items:end;}}
        .ctrl-group label{{display:block;font-size:0.68rem;text-transform:uppercase;letter-spacing:1.5px;color:rgba(255,255,255,0.6);font-weight:600;margin-bottom:0.35rem;}}
        .ctrl-group select{{width:100%;padding:0.5rem 0.7rem;border:1.5px solid rgba(255,255,255,0.14);border-radius:var(--radius-sm);font-family:inherit;font-size:0.85rem;color:#fff;background:rgba(0,0,0,0.35);outline:none;transition:border-color .2s;}}
        .ctrl-group select:focus{{border-color:var(--reef-teal);}}

        .range-row{{display:flex;gap:0.5rem;align-items:center;}}
        .range-labels{{display:flex;justify-content:space-between;font-size:0.7rem;color:rgba(255,255,255,0.6);margin-top:0.2rem;font-weight:500;}}
        input[type="range"]{{-webkit-appearance:none;width:100%;height:6px;border-radius:3px;background:linear-gradient(90deg,var(--reef-teal),var(--lagoon));border:none;padding:0;outline:none;}}
        input[type="range"]::-webkit-slider-thumb{{-webkit-appearance:none;width:18px;height:18px;border-radius:50%;background:var(--reef-teal);border:3px solid #000;box-shadow:0 2px 8px rgba(0,0,0,0.5);cursor:pointer;}}

        .toggle-row{{display:flex;gap:0.4rem;flex-wrap:wrap;}}
        .toggle-btn{{padding:0.4rem 0.9rem;border-radius:20px;border:1.5px solid rgba(255,255,255,0.14);background:rgba(0,0,0,0.35);font-family:inherit;font-size:0.8rem;font-weight:500;color:rgba(255,255,255,0.75);cursor:pointer;transition:all .2s;}}
        .toggle-btn:hover{{border-color:var(--reef-teal);color:var(--reef-teal);}}
        .toggle-btn.active{{background:var(--reef-teal);border-color:var(--reef-teal);color:#fff;}}

        .charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-bottom:2rem;}}
        .chart-card{{background:var(--card-bg);border:1px solid var(--card-border);border-radius:var(--radius);padding:1.4rem;box-shadow:var(--shadow);transition:box-shadow .25s;}}
        .chart-card:hover{{box-shadow:var(--shadow-hover);}}
        .chart-card.full{{grid-column:1/-1;}}
        .chart-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:0.8rem;}}
        .chart-title{{font-weight:600;font-size:0.95rem;color:#fff;}}
        .chart-subtitle{{font-size:0.72rem;color:rgba(255,255,255,0.6);margin-top:0.1rem;}}
        .chart-wrap{{position:relative;height:320px;}}
        .chart-wrap.tall{{height:400px;}}

        .footer{{background:transparent;color:rgba(255,255,255,0.45);text-align:center;padding:1.8rem 1rem;font-size:0.78rem;}}
        .footer a{{color:var(--lagoon);text-decoration:none;}}

        @media(max-width:900px){{.charts-grid{{grid-template-columns:1fr;}}}}
        @media(max-width:600px){{.stats-row{{grid-template-columns:1fr 1fr;}}.controls-grid{{grid-template-columns:1fr;}}}}

        @keyframes fadeUp{{from{{opacity:0;transform:translateY(16px);}}to{{opacity:1;transform:translateY(0);}}}}
        .stat-card,.chart-card,.controls-panel{{animation:fadeUp 0.45s ease-out both;}}
        .stat-card:nth-child(2){{animation-delay:0.06s;}}.stat-card:nth-child(3){{animation-delay:0.12s;}}
        .stat-card:nth-child(4){{animation-delay:0.18s;}}.stat-card:nth-child(5){{animation-delay:0.24s;}}
    </style>
</head>
<body>
<!-- REMOVED: wave separator that created the curved header band -->
<div class="main-bg"><div class="container">
    <div class="stats-row">
        <div class="stat-card"><div class="stat-label">Commercial EV</div><div class="stat-value">{comm_total_display}</div><div class="stat-sub">{comm_years_display}</div></div>
        <div class="stat-card"><div class="stat-label">Non-Commercial EV</div><div class="stat-value">{noncomm_total_display}</div><div class="stat-sub">{noncomm_years_display}</div></div>
        <div class="stat-card"><div class="stat-label">Top Species Group</div><div class="stat-value" style="font-size:1.2rem">{s['top_species']}</div><div class="stat-sub">{top_species_val_display}</div></div>
        <div class="stat-card"><div class="stat-label">Top County</div><div class="stat-value" style="font-size:1.2rem">{s['top_county']}</div><div class="stat-sub">{top_county_val_display}</div></div>
        <div class="stat-card"><div class="stat-label">Total Records</div><div class="stat-value">{s['total_records']:,}</div><div class="stat-sub">After cleaning</div></div>
    </div>
    <div class="controls-panel"><div class="controls-title">&#9881; Filters &amp; Controls</div><div class="controls-grid">
        <div class="ctrl-group"><label>Data Source</label><div class="toggle-row" id="srcToggle"><button class="toggle-btn active" data-v="both">Both</button><button class="toggle-btn" data-v="commercial">Commercial</button><button class="toggle-btn" data-v="noncommercial">Non-Commercial</button></div></div>
        <div class="ctrl-group"><label>County / Island</label><select id="fRegion"><option value="all">All Regions</option></select></div>
        <div class="ctrl-group"><label>Species Group</label><select id="fSpecies"><option value="all">All Species</option></select></div>
        <div class="ctrl-group"><label>Ecosystem</label><select id="fEco"><option value="all">All Ecosystems</option></select></div>
        <div class="ctrl-group"><label>Year Range: <strong id="yrLabel">{year_min} &ndash; {year_max}</strong></label><div class="range-row"><input type="range" id="yrMin" min="{year_min}" max="{year_max}" value="{year_min}"><input type="range" id="yrMax" min="{year_min}" max="{year_max}" value="{year_max}"></div><div class="range-labels"><span id="yrMinL">{year_min}</span><span id="yrMaxL">{year_max}</span></div></div>
    </div></div>
    <div class="charts-grid">
        <div class="chart-card full"><div class="chart-header"><div><div class="chart-title">Exchange Value Over Time</div><div class="chart-subtitle">Annual totals &middot; Millions USD</div></div><div class="toggle-row" id="timeType"><button class="toggle-btn active" data-v="line">Line</button><button class="toggle-btn" data-v="bar">Bar</button></div></div><div class="chart-wrap tall"><canvas id="cTime"></canvas></div></div>
        <div class="chart-card"><div class="chart-header"><div><div class="chart-title">By Species Group</div><div class="chart-subtitle">Total exchange value</div></div></div><div class="chart-wrap"><canvas id="cSpecies"></canvas></div></div>
        <div class="chart-card"><div class="chart-header"><div><div class="chart-title">By Ecosystem Type</div><div class="chart-subtitle">Reef vs Open Ocean</div></div></div><div class="chart-wrap"><canvas id="cEco"></canvas></div></div>
        <div class="chart-card"><div class="chart-header"><div><div class="chart-title">By County</div><div class="chart-subtitle">Geographic distribution</div></div></div><div class="chart-wrap"><canvas id="cCounty"></canvas></div></div>
        <div class="chart-card"><div class="chart-header"><div><div class="chart-title">By Island (Non-Commercial)</div><div class="chart-subtitle">Herbivore reef fisheries</div></div></div><div class="chart-wrap"><canvas id="cIsland"></canvas></div></div>
        <div class="chart-card full"><div class="chart-header"><div><div class="chart-title">Species Group Trends Over Time</div><div class="chart-subtitle">Stacked area &middot; Millions USD</div></div></div><div class="chart-wrap tall"><canvas id="cStacked"></canvas></div></div>
    </div>
</div></div>
<script>
const DATA_COMMERCIAL={comm_json};
const DATA_NONCOMMERCIAL={noncomm_json};
const S={{source:'both',region:'all',species:'all',ecosystem:'all',yearMin:{year_min},yearMax:{year_max}}};
const C={{}};
const PAL={{comm:'#1B998B',noncomm:'#FF6B6B',species:['#1B998B','#2EC4B6','#F77F00','#FFD166','#D62828','#0B1D3A','#133B5C','#8E44AD'],eco:['#1B998B','#FF6B6B','#F77F00'],county:['#133B5C','#1B998B','#F77F00','#FFD166','#D62828'],island:['#0B1D3A','#133B5C','#1B998B','#2EC4B6','#F77F00','#FFD166']}};
function fmtM(v){{if(v>=1e3)return'$'+(v/1e3).toFixed(2)+'B';if(v>=1)return'$'+v.toFixed(1)+'M';if(v>=0.001)return'$'+(v*1000).toFixed(0)+'K';return'$'+(v*1e6).toFixed(0);}}
function sumByField(data,field){{const m={{}};data.forEach(d=>{{const k=d[field]||'Unknown';m[k]=(m[k]||0)+d.exchange_value;}});return Object.entries(m).sort((a,b)=>b[1]-a[1]);}}
function sumByYear(data){{const m={{}};data.forEach(d=>{{m[d.year]=(m[d.year]||0)+d.exchange_value;}});return m;}}
function sumByYearAndField(data,field){{const g={{}};data.forEach(d=>{{const k=d[field]||'Unknown';if(!g[k])g[k]={{}};g[k][d.year]=(g[k][d.year]||0)+d.exchange_value;}});return g;}}
function filtered(){{let data=[];if(S.source!=='noncommercial')data=data.concat(DATA_COMMERCIAL);if(S.source!=='commercial')data=data.concat(DATA_NONCOMMERCIAL);return data.filter(d=>d.year>=S.yearMin&&d.year<=S.yearMax&&(S.region==='all'||d.county===S.region||d.island===S.region)&&(S.species==='all'||d.species_group===S.species)&&(S.ecosystem==='all'||d.ecosystem_type===S.ecosystem));}}
function populateFilters(){{const all=[...DATA_COMMERCIAL,...DATA_NONCOMMERCIAL];const regions=new Set(),species=new Set(),ecos=new Set();all.forEach(d=>{{if(d.county)regions.add(d.county);if(d.island)regions.add(d.island);if(d.species_group)species.add(d.species_group);if(d.ecosystem_type)ecos.add(d.ecosystem_type);}});fillSelect('fRegion',regions,'All Regions');fillSelect('fSpecies',species,'All Species');fillSelect('fEco',ecos,'All Ecosystems');}}
function fillSelect(id,vals,allLabel){{const el=document.getElementById(id);el.innerHTML='<option value="all">'+allLabel+'</option>';[...vals].sort().forEach(v=>{{el.innerHTML+='<option value="'+v+'">'+v+'</option>';}});}}
Chart.defaults.font.family="'IBM Plex Sans',sans-serif";Chart.defaults.font.size=12;Chart.defaults.color='rgba(255,255,255,0.7)';Chart.defaults.plugins.legend.labels.usePointStyle=true;Chart.defaults.plugins.tooltip.backgroundColor='rgba(0,0,0,0.9)';Chart.defaults.plugins.tooltip.cornerRadius=8;Chart.defaults.plugins.tooltip.padding=12;
const ttM={{callbacks:{{label:ctx=>ctx.dataset.label+': '+fmtM(ctx.parsed.y)}}}};
const ttMx={{callbacks:{{label:ctx=>ctx.dataset.label+': '+fmtM(ctx.parsed.x)}}}};
function initCharts(){{C.time=new Chart(document.getElementById('cTime'),{{type:'line',data:{{labels:[],datasets:[]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:ttM}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'Exchange Value (Millions USD)'}},grid:{{color:'rgba(255,255,255,0.08)'}}}},x:{{grid:{{display:false}}}}}}}}}});C.species=new Chart(document.getElementById('cSpecies'),{{type:'doughnut',data:{{labels:[],datasets:[{{data:[],backgroundColor:PAL.species,borderWidth:2,borderColor:'#000'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom'}},tooltip:{{callbacks:{{label:ctx=>ctx.label+': '+fmtM(ctx.parsed)}}}}}}}}}});C.eco=new Chart(document.getElementById('cEco'),{{type:'polarArea',data:{{labels:[],datasets:[{{data:[],backgroundColor:PAL.eco.map(c=>c+'99'),borderColor:PAL.eco,borderWidth:2}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom'}},tooltip:{{callbacks:{{label:ctx=>ctx.label+': '+fmtM(ctx.parsed)}}}}}},scales:{{r:{{ticks:{{display:false}}}}}}}}}});C.county=new Chart(document.getElementById('cCounty'),{{type:'bar',data:{{labels:[],datasets:[{{data:[],backgroundColor:PAL.county,borderRadius:6,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}},tooltip:ttMx}},scales:{{x:{{beginAtZero:true,title:{{display:true,text:'Millions USD'}},grid:{{color:'rgba(255,255,255,0.08)'}}}},y:{{grid:{{display:false}}}}}}}}}});C.island=new Chart(document.getElementById('cIsland'),{{type:'bar',data:{{labels:[],datasets:[{{data:[],backgroundColor:PAL.island,borderRadius:6,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:ttM}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'Millions USD'}},grid:{{color:'rgba(255,255,255,0.08)'}}}},x:{{grid:{{display:false}}}}}}}}}});C.stacked=new Chart(document.getElementById('cStacked'),{{type:'line',data:{{labels:[],datasets:[]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:ttM,legend:{{position:'bottom'}}}},scales:{{y:{{stacked:true,beginAtZero:true,title:{{display:true,text:'Exchange Value (Millions USD)'}},grid:{{color:'rgba(255,255,255,0.08)'}}}},x:{{grid:{{display:false}}}}}}}}}});}}
function updateAll(){{const f=filtered();const comm=f.filter(d=>d.type==='commercial');const noncomm=f.filter(d=>d.type==='noncommercial');const chartType=document.querySelector('#timeType .toggle-btn.active')?.dataset.v||'line';const cY=sumByYear(comm),nY=sumByYear(noncomm);const allY=[...new Set([...Object.keys(cY),...Object.keys(nY)])].sort();C.time.config.type=chartType;C.time.data.labels=allY;C.time.data.datasets=[];if(S.source!=='noncommercial'){{C.time.data.datasets.push({{label:'Commercial',data:allY.map(y=>(cY[y]||0)/1e6),borderColor:PAL.comm,backgroundColor:chartType==='bar'?PAL.comm+'CC':PAL.comm+'22',fill:chartType==='line',tension:0.3,pointRadius:chartType==='line'?3:0,borderWidth:2,borderRadius:chartType==='bar'?4:0}});}}if(S.source!=='commercial'){{C.time.data.datasets.push({{label:'Non-Commercial',data:allY.map(y=>(nY[y]||0)/1e6),borderColor:PAL.noncomm,backgroundColor:chartType==='bar'?PAL.noncomm+'CC':PAL.noncomm+'22',fill:chartType==='line',tension:0.3,pointRadius:chartType==='line'?3:0,borderWidth:2,borderRadius:chartType==='bar'?4:0}});}}C.time.update();const sp=sumByField(f,'species_group');C.species.data.labels=sp.map(s=>s[0]);C.species.data.datasets[0].data=sp.map(s=>s[1]/1e6);C.species.data.datasets[0].backgroundColor=PAL.species.slice(0,sp.length);C.species.update();const ec=sumByField(f,'ecosystem_type');C.eco.data.labels=ec.map(s=>s[0]);C.eco.data.datasets[0].data=ec.map(s=>s[1]/1e6);C.eco.update();const co=sumByField(f,'county').filter(s=>s[0]&&s[0]!=='Unknown');C.county.data.labels=co.map(s=>s[0]);C.county.data.datasets[0].data=co.map(s=>s[1]/1e6);C.county.data.datasets[0].backgroundColor=PAL.county.slice(0,co.length);C.county.update();const isl=sumByField(noncomm,'island').filter(s=>s[0]&&s[0]!=='Unknown'&&s[0]!=='');C.island.data.labels=isl.map(s=>s[0]);C.island.data.datasets[0].data=isl.map(s=>s[1]/1e6);C.island.data.datasets[0].backgroundColor=PAL.island.slice(0,isl.length);C.island.update();const spOT=sumByYearAndField(f,'species_group');const sYears=[...new Set(f.map(d=>d.year))].sort();const spNames=Object.keys(spOT);C.stacked.data.labels=sYears;C.stacked.data.datasets=spNames.map((sp,i)=>({{label:sp,data:sYears.map(y=>(spOT[sp][y]||0)/1e6),backgroundColor:PAL.species[i%PAL.species.length]+'55',borderColor:PAL.species[i%PAL.species.length],fill:true,tension:0.3,pointRadius:0,borderWidth:1.5}}));C.stacked.options.scales.y.stacked=true;C.stacked.update();}}
function setupListeners(){{document.querySelectorAll('#srcToggle .toggle-btn').forEach(btn=>{{btn.addEventListener('click',()=>{{document.querySelectorAll('#srcToggle .toggle-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');S.source=btn.dataset.v;updateAll();}});}});document.querySelectorAll('#timeType .toggle-btn').forEach(btn=>{{btn.addEventListener('click',()=>{{document.querySelectorAll('#timeType .toggle-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');C.time.destroy();C.time=new Chart(document.getElementById('cTime'),{{type:btn.dataset.v,data:{{labels:[],datasets:[]}},options:{{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{tooltip:ttM}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'Exchange Value (Millions USD)'}},grid:{{color:'rgba(255,255,255,0.08)'}}}},x:{{grid:{{display:false}}}}}}}}}});updateAll();}});}});['fRegion','fSpecies','fEco'].forEach(id=>{{document.getElementById(id).addEventListener('change',e=>{{const key=id==='fRegion'?'region':id==='fSpecies'?'species':'ecosystem';S[key]=e.target.value;updateAll();}});}});const yMin=document.getElementById('yrMin');const yMax=document.getElementById('yrMax');function updateYr(){{let lo=+yMin.value,hi=+yMax.value;if(lo>hi){{[lo,hi]=[hi,lo];yMin.value=lo;yMax.value=hi;}}S.yearMin=lo;S.yearMax=hi;document.getElementById('yrLabel').innerHTML=lo+' &ndash; '+hi;document.getElementById('yrMinL').textContent=lo;document.getElementById('yrMaxL').textContent=hi;updateAll();}}yMin.addEventListener('input',updateYr);yMax.addEventListener('input',updateYr);}}
document.addEventListener('DOMContentLoaded',()=>{{populateFilters();initCharts();setupListeners();updateAll();}});
</script>
</body>
</html>'''


#*****************************************************************
#
#  STANDALONE EXECUTION
#
#  DESCRIPTION:   Runs the dashboard generator independently for testing against
#                 the default data/cleaned directory. Sets up basic logging and
#                 prints an error message if no cleaned CSVs are found.
#
#*****************************************************************

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    gen = DashboardGenerator(
        data_dir='data/cleaned',
        output_dir='data/cleaned'
    )
    result = gen.generate()
    if not result:
        print("ERROR: No data found. Run the cleaning pipeline first.")
        print("  Expected files in data/cleaned/:")
        print("  cleaned_commercial_*.csv")
        print("  cleaned_noncommercial_*.csv")
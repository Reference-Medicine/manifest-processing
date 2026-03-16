"""
Core manifest processing engine for Reference Medicine.
Handles ingestion, cleaning, mapping, calculations, ID generation, and export.
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


CONFIG_PATH = Path(__file__).parent / "config" / "default_mapping.json"


def load_config(path=None):
    """Load the mapping configuration from JSON."""
    p = Path(path) if path else CONFIG_PATH
    with open(p) as f:
        return json.load(f)


def save_config(config, path=None):
    """Save the mapping configuration to JSON."""
    p = Path(path) if path else CONFIG_PATH
    with open(p, "w") as f:
        json.dump(config, f, indent=2)


# ---------------------------------------------------------------------------
# Date/time cleaning
# ---------------------------------------------------------------------------

def clean_date_string(val):
    """Replace periods with slashes in date strings and normalize to mm/dd/yyyy."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.strftime("%m/%d/%Y")
    s = str(val).strip()
    if not s:
        return None
    # Replace periods with slashes
    s = s.replace(".", "/")
    return s


def clean_time_string(val):
    """Normalize time values to HH:MM format."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.strftime("%H:%M")
    from datetime import time as dt_time
    if isinstance(val, dt_time):
        return val.strftime("%H:%M")
    s = str(val).strip()
    if not s:
        return None
    return s


def clean_numeric_string(val):
    """Replace commas with periods in numeric fields."""
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    return s


def parse_date(date_str):
    """Try multiple date formats and return a datetime.date or None."""
    if date_str is None:
        return None
    if isinstance(date_str, datetime):
        return date_str.date()
    import datetime as dt_module
    if isinstance(date_str, dt_module.date):
        return date_str
    s = str(date_str).strip().replace(".", "/")
    if not s:
        return None

    formats = [
        "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
        "%m/%d/%y", "%d/%m/%y", "%Y-%m-%d",
        "%m-%d-%Y", "%d-%m-%Y",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(s, fmt)
            # Heuristic: if first number > 12, it's likely dd/mm
            # We default to mm/dd/yyyy but allow ambiguity flags
            return parsed.date()
        except ValueError:
            continue
    return None


def parse_time(time_str):
    """Parse a time string to datetime.time."""
    if time_str is None:
        return None
    from datetime import time as dt_time
    if isinstance(time_str, dt_time):
        return time_str
    if isinstance(time_str, datetime):
        return time_str.time()
    s = str(time_str).strip()
    if not s:
        return None
    for fmt in ["%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"]:
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def combine_datetime(date_val, time_val):
    """Combine a date and time into a formatted datetime string mm/dd/yyyy HH:MM."""
    d = parse_date(date_val)
    t = parse_time(time_val)
    if d is None:
        return None
    if t is None:
        return d.strftime("%m/%d/%Y")
    return datetime.combine(d, t).strftime("%m/%d/%Y %H:%M")


def parse_combined_datetime(val):
    """Parse a combined date/time string back to datetime object."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    for fmt in ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def time_diff_hours(start_str, end_str):
    """Calculate difference in hours between two datetime strings."""
    start = parse_combined_datetime(start_str)
    end = parse_combined_datetime(end_str)
    if start is None or end is None:
        return None
    diff = end - start
    hours = diff.total_seconds() / 3600
    return round(hours, 2)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def read_manifest(file_path_or_buffer, sheet_name=0):
    """Read an Excel manifest file into a DataFrame."""
    df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name)
    # Strip whitespace from column names
    df.columns = [str(c).strip() for c in df.columns]
    return df


def detect_unrecognized_columns(df, config, supplier):
    """Find columns in the import file that are not in the mapping config."""
    supplier_key = supplier.lower()
    known_columns = set()
    for m in config["column_mappings"]:
        col = m.get(supplier_key) or m.get(supplier.lower())
        if col:
            known_columns.add(col.strip())

    unrecognized = []
    for col in df.columns:
        if col.strip() not in known_columns:
            unrecognized.append(col.strip())
    return unrecognized


def add_unrecognized_to_config(config, supplier, columns):
    """Add unrecognized columns to the mapping config with null output (for user to map)."""
    supplier_key = supplier.lower()
    for col in columns:
        entry = {"csd": None, "biomedica": None, "output": None, "type": "string",
                 "category": "unmapped", "comments": "auto-detected"}
        entry[supplier_key] = col
        config["column_mappings"].append(entry)
    return config


# ---------------------------------------------------------------------------
# Mapping: input -> output
# ---------------------------------------------------------------------------

def build_column_map(config, supplier):
    """Build a dict: {input_column_name: mapping_entry} for the given supplier."""
    supplier_key = supplier.lower()
    col_map = {}
    for m in config["column_mappings"]:
        input_col = m.get(supplier_key)
        if input_col:
            col_map[input_col.strip()] = m
    return col_map


def map_row(row, col_map, config, supplier):
    """Map a single row from input columns to output columns, applying cleaning."""
    output = {}
    for input_col, mapping in col_map.items():
        output_col = mapping.get("output")
        if not output_col:
            continue
        val = row.get(input_col)
        field_type = mapping.get("type", "string")

        # Apply cleaning based on type
        if field_type == "date":
            val = clean_date_string(val)
        elif field_type == "time":
            val = clean_time_string(val)
        elif field_type in ("integer", "float"):
            val = clean_numeric_string(val)

        output[output_col] = val
    return output


def apply_calculations(row_data, config):
    """Apply all configured calculations to a mapped row."""
    for calc in config.get("calculations", []):
        output_field = calc["output"]
        if calc["type"] == "combine_datetime":
            date_val = row_data.get(calc["date_field"])
            time_val = row_data.get(calc["time_field"])
            row_data[output_field] = combine_datetime(date_val, time_val)
        elif calc["type"] == "time_diff_hours":
            start = row_data.get(calc["start"])
            end = row_data.get(calc["end"])
            row_data[output_field] = time_diff_hours(start, end)
    return row_data


# ---------------------------------------------------------------------------
# Specimen ID generation (CSD)
# ---------------------------------------------------------------------------

def generate_specimen_ids(donor_id, count, prefix):
    """Generate specimen IDs: <Donor ID> - <PREFIX>1, <PREFIX>2, ..."""
    if not donor_id or not count:
        return []
    try:
        count = int(float(str(count).replace(",", ".")))
    except (ValueError, TypeError):
        return []
    return [f"{donor_id} - {prefix}{i}" for i in range(1, count + 1)]


# ---------------------------------------------------------------------------
# CSD processing: 1 donor row -> multiple specimen rows
# ---------------------------------------------------------------------------

def expand_csd_specimens(mapped_row, config):
    """Expand a single CSD donor row into individual specimen rows by type."""
    specimens_by_type = {}
    donor_id = mapped_row.get("Donor ID", "")
    id_rules = config.get("specimen_id_rules", {})

    for spec_type, rule in id_rules.items():
        count_field = rule.get("count_field")
        prefix = rule.get("prefix", "")

        if count_field is None:
            # Special handling (e.g., stool with barcodes)
            continue

        count_val = mapped_row.get(count_field)
        if count_val is None:
            continue
        try:
            count = int(float(str(count_val).replace(",", ".")))
        except (ValueError, TypeError):
            continue

        if count <= 0:
            continue

        ids = generate_specimen_ids(donor_id, count, prefix)
        specimen_rows = []
        for sid in ids:
            row = dict(mapped_row)
            row["Specimen ID"] = sid
            specimen_rows.append(row)
        specimens_by_type[spec_type] = specimen_rows

    # Handle stool specimens (they have individual barcodes)
    stool_rows = []
    for i in [1, 2]:
        barcode_key = f"Stool No. {i} barcode"
        weight_key = f"Stool No. {i} weight"
        barcode = mapped_row.get(barcode_key)
        if barcode:
            row = dict(mapped_row)
            row["Specimen ID"] = barcode
            stool_rows.append(row)
    if stool_rows:
        specimens_by_type["stool"] = stool_rows

    return specimens_by_type


# ---------------------------------------------------------------------------
# Biomedica processing: rows are already per-specimen
# ---------------------------------------------------------------------------

def categorize_biomedica_specimen(row, config):
    """Determine specimen category from a Biomedica row based on Type of collection or Specimen type."""
    spec_type = str(row.get("Specimen type", row.get("Type of collection", ""))).strip().lower()

    # Order matters: check more specific terms before generic ones
    checks = [
        ("buffy coat", "buffy_coat"),
        ("buffy", "buffy_coat"),
        ("whole blood", "whole_blood"),
        ("plasma", "plasma"),
        ("serum", "serum"),
        ("urine", "urine"),
        ("stool", "stool"),
        ("feces", "stool"),
        ("ffpe", "tissue"),
        ("tissue", "tissue"),
        ("formalin", "tissue"),
        ("block", "tissue"),
        ("fresh frozen", "tissue"),
        ("ff", "tissue"),
        ("blood", "whole_blood"),
    ]

    for keyword, category in checks:
        if keyword in spec_type:
            return category
    return "unknown"


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def process_manifest(files, supplier, config):
    """
    Process one or more manifest files for a given supplier.
    Returns:
        - cases_df: DataFrame of cases
        - specimen_dfs: dict of {specimen_type: DataFrame}
        - wo_summary: DataFrame with WO summary
        - warnings: list of warning strings
        - unrecognized_cols: list of unrecognized column names
    """
    config = json.loads(json.dumps(config))  # deep copy
    warnings = []
    all_unrecognized = []

    # Read and combine input files
    dfs = []
    for f in files:
        df = read_manifest(f)
        unrecognized = detect_unrecognized_columns(df, config, supplier)
        all_unrecognized.extend(unrecognized)
        if unrecognized:
            config = add_unrecognized_to_config(config, supplier, unrecognized)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    col_map = build_column_map(config, supplier)

    # Map and clean all rows
    mapped_rows = []
    for _, row in combined_df.iterrows():
        mapped = map_row(row, col_map, config, supplier)
        mapped = apply_calculations(mapped, config)
        mapped_rows.append(mapped)

    if not mapped_rows:
        return pd.DataFrame(), {}, pd.DataFrame(), ["No data rows found"], all_unrecognized

    all_mapped_df = pd.DataFrame(mapped_rows)

    # Build cases (deduplicated by Donor ID)
    export_templates = config.get("export_templates", {})
    cases_cols = export_templates.get("Cases", {}).get("columns", [])
    available_cases_cols = [c for c in cases_cols if c in all_mapped_df.columns]

    if "Donor ID" in all_mapped_df.columns:
        cases_df = all_mapped_df[available_cases_cols].drop_duplicates(
            subset=["Donor ID"], keep="first"
        ).reset_index(drop=True)
    else:
        cases_df = all_mapped_df[available_cases_cols].drop_duplicates().reset_index(drop=True)

    # Build specimen DataFrames
    specimen_dfs = {}

    if supplier.upper() == "CSD":
        # Expand each donor row into specimen rows
        all_specimens = {}
        for _, row in all_mapped_df.iterrows():
            row_dict = row.to_dict()
            expanded = expand_csd_specimens(row_dict, config)
            for spec_type, spec_rows in expanded.items():
                all_specimens.setdefault(spec_type, []).extend(spec_rows)

        # Map spec_type keys to export template names
        type_to_template = {
            "tissue_ffpe_tumor": "Tissue", "tissue_ffpe_normal": "Tissue",
            "tissue_ff": "Tissue",
            "whole_blood": "Whole Blood",
            "plasma": "Plasma",
            "buffy_coat": "Buffy Coat",
            "serum": "Serum",
            "urine_5ml": "Urine", "urine_2ml": "Urine",
            "stool": "Stool",
        }

        for spec_type, rows in all_specimens.items():
            template_name = type_to_template.get(spec_type, spec_type)
            template_cols = export_templates.get(template_name, {}).get("columns", [])
            df = pd.DataFrame(rows)
            available_cols = [c for c in template_cols if c in df.columns]
            if available_cols:
                spec_df = df[available_cols]
                if template_name in specimen_dfs:
                    specimen_dfs[template_name] = pd.concat(
                        [specimen_dfs[template_name], spec_df], ignore_index=True
                    )
                else:
                    specimen_dfs[template_name] = spec_df

    elif supplier.upper() == "BIOMEDICA":
        # Each row is already a specimen; categorize and route
        for _, row in all_mapped_df.iterrows():
            row_dict = row.to_dict()
            category = categorize_biomedica_specimen(row_dict, config)

            template_map = {
                "tissue": "Tissue", "whole_blood": "Whole Blood",
                "plasma": "Plasma", "buffy_coat": "Buffy Coat",
                "serum": "Serum", "urine": "Urine", "stool": "Stool",
            }
            template_name = template_map.get(category, "Unknown")
            template_cols = export_templates.get(template_name, {}).get("columns", [])
            available_cols = [c for c in template_cols if c in row_dict]
            if available_cols:
                row_filtered = {c: row_dict[c] for c in available_cols}
                specimen_dfs.setdefault(template_name, []).append(row_filtered)

        # Convert lists to DataFrames
        for key in list(specimen_dfs.keys()):
            if isinstance(specimen_dfs[key], list):
                specimen_dfs[key] = pd.DataFrame(specimen_dfs[key])

    # Validation warnings
    for _, row in all_mapped_df.iterrows():
        donor = row.get("Donor ID", "?")
        hrs_blood_op = row.get("Hours between blood collection and operation")
        if hrs_blood_op is not None and isinstance(hrs_blood_op, (int, float)) and hrs_blood_op < 0:
            warnings.append(
                f"Donor {donor}: Blood collection appears to be AFTER operation "
                f"({hrs_blood_op} hrs). Blood collection must be before operation."
            )
        hrs_formalin = row.get("Hours in formalin")
        if hrs_formalin is not None and isinstance(hrs_formalin, (int, float)) and hrs_formalin < 0:
            warnings.append(f"Donor {donor}: Negative formalin time ({hrs_formalin} hrs).")

    # WO Summary
    wo_summary = build_wo_summary(cases_df, specimen_dfs)

    return cases_df, specimen_dfs, wo_summary, warnings, list(set(all_unrecognized))


def build_wo_summary(cases_df, specimen_dfs):
    """Build a summary table by Work Order #."""
    rows = []
    wo_col = "WO #"

    if wo_col not in cases_df.columns or cases_df.empty:
        return pd.DataFrame()

    for wo, group in cases_df.groupby(wo_col):
        row = {"WO #": wo, "# Donors/Cases": len(group)}
        for spec_name, spec_df in specimen_dfs.items():
            if wo_col in spec_df.columns:
                count = len(spec_df[spec_df[wo_col] == wo])
                row[f"# {spec_name}"] = count
        rows.append(row)

    return pd.DataFrame(rows)

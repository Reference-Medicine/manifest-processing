"""
Reference Medicine - Manifest Processing System
Streamlit GUI for ingesting supplier manifests and generating LIMS export files.
"""

import io
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from processing_engine import (
    load_config,
    save_config,
    process_manifest,
    CONFIG_PATH,
)

st.set_page_config(page_title="RM Manifest Processor", layout="wide")


# ---------------------------------------------------------------------------
# Session state initialization
# ---------------------------------------------------------------------------

def init_state():
    if "config" not in st.session_state:
        st.session_state.config = load_config()
    if "processed" not in st.session_state:
        st.session_state.processed = False
    if "cases_df" not in st.session_state:
        st.session_state.cases_df = None
    if "specimen_dfs" not in st.session_state:
        st.session_state.specimen_dfs = {}
    if "wo_summary" not in st.session_state:
        st.session_state.wo_summary = None
    if "warnings" not in st.session_state:
        st.session_state.warnings = []
    if "unrecognized" not in st.session_state:
        st.session_state.unrecognized = []


init_state()
config = st.session_state.config


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

st.sidebar.title("RM Manifest Processor")
page = st.sidebar.radio("Navigate", [
    "Process Manifests",
    "Column Mapping",
    "Export Templates",
    "Specimen ID Rules",
])


# ---------------------------------------------------------------------------
# Page: Process Manifests
# ---------------------------------------------------------------------------

if page == "Process Manifests":
    st.title("Process Supplier Manifests")

    col1, col2 = st.columns(2)
    with col1:
        supplier = st.selectbox("Supplier", ["CSD", "Biomedica"])
    with col2:
        if supplier == "CSD":
            st.info("Upload 1 manifest file (1 row = 1 donor)")
        else:
            st.info("Upload 1+ manifest files (1 per Work Order, 1 row = 1 specimen)")

    uploaded_files = st.file_uploader(
        "Upload manifest Excel file(s)",
        type=["xlsx", "xls"],
        accept_multiple_files=(supplier == "Biomedica"),
        key="manifest_upload",
    )

    # Normalize to list
    if uploaded_files is not None:
        if not isinstance(uploaded_files, list):
            uploaded_files = [uploaded_files]
    else:
        uploaded_files = []

    if uploaded_files and st.button("Process Manifests", type="primary"):
        with st.spinner("Processing..."):
            cases_df, specimen_dfs, wo_summary, warnings, unrecognized = process_manifest(
                uploaded_files, supplier, config
            )

            st.session_state.cases_df = cases_df
            st.session_state.specimen_dfs = specimen_dfs
            st.session_state.wo_summary = wo_summary
            st.session_state.warnings = warnings
            st.session_state.unrecognized = unrecognized
            st.session_state.processed = True

            # Auto-add unrecognized columns to config
            if unrecognized:
                from processing_engine import add_unrecognized_to_config
                config = add_unrecognized_to_config(config, supplier, unrecognized)
                st.session_state.config = config
                save_config(config)

        st.success("Processing complete!")

    # Display results
    if st.session_state.processed:
        # Warnings
        if st.session_state.warnings:
            st.subheader("Warnings / Data Discrepancies")
            for w in st.session_state.warnings:
                st.warning(w)

        # Unrecognized columns
        if st.session_state.unrecognized:
            st.subheader("Unrecognized Columns")
            st.write(
                "The following columns were found in the import file but are not mapped. "
                "They have been added to the Column Mapping page for you to configure."
            )
            for c in st.session_state.unrecognized:
                st.code(c)

        # WO Summary
        if st.session_state.wo_summary is not None and not st.session_state.wo_summary.empty:
            st.subheader("Work Order Summary")
            st.dataframe(st.session_state.wo_summary, use_container_width=True, hide_index=True)

        # Cases preview & download
        if st.session_state.cases_df is not None and not st.session_state.cases_df.empty:
            st.subheader("Cases Export")
            st.dataframe(st.session_state.cases_df, use_container_width=True, hide_index=True)
            st.download_button(
                "Download Cases.xlsx",
                data=df_to_excel_bytes(st.session_state.cases_df),
                file_name="Cases.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Specimen exports
        for spec_name, spec_df in st.session_state.specimen_dfs.items():
            if spec_df is not None and not spec_df.empty:
                st.subheader(f"{spec_name} Export")
                st.dataframe(spec_df, use_container_width=True, hide_index=True)
                st.download_button(
                    f"Download {spec_name}.xlsx",
                    data=df_to_excel_bytes(spec_df),
                    file_name=f"{spec_name}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_{spec_name}",
                )

        # Download all as zip
        if st.session_state.specimen_dfs:
            st.subheader("Download All Exports")
            zip_bytes = create_zip_download(
                st.session_state.cases_df,
                st.session_state.specimen_dfs,
            )
            st.download_button(
                "Download All (ZIP)",
                data=zip_bytes,
                file_name="manifest_exports.zip",
                mime="application/zip",
            )


# ---------------------------------------------------------------------------
# Page: Column Mapping
# ---------------------------------------------------------------------------

elif page == "Column Mapping":
    st.title("Column Mapping Configuration")
    st.write("Map supplier input columns to output fields. Changes are saved automatically.")

    # Filter by category
    categories = sorted(set(m.get("category", "unmapped") for m in config["column_mappings"]))
    selected_category = st.selectbox("Filter by category", ["all"] + categories)

    mappings = config["column_mappings"]
    if selected_category != "all":
        mappings = [m for m in config["column_mappings"] if m.get("category") == selected_category]

    # All possible output fields for dropdown
    all_output_fields = sorted(set(
        m["output"] for m in config["column_mappings"] if m.get("output")
    ))

    edited = False
    for i, m in enumerate(mappings):
        # Find index in full config
        full_idx = config["column_mappings"].index(m)

        with st.expander(
            f"{m.get('output') or m.get('csd') or m.get('biomedica') or 'Unmapped'} "
            f"[{m.get('category', 'unmapped')}]",
            expanded=(m.get("category") == "unmapped"),
        ):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                new_csd = st.text_input("CSD column", value=m.get("csd") or "", key=f"csd_{full_idx}")
            with col2:
                new_bio = st.text_input("Biomedica column", value=m.get("biomedica") or "", key=f"bio_{full_idx}")
            with col3:
                new_output = st.text_input("Output field", value=m.get("output") or "", key=f"out_{full_idx}")
            with col4:
                type_options = ["string", "integer", "float", "date", "time", "date/time", "boolean"]
                current_type = m.get("type", "string")
                if current_type not in type_options:
                    type_options.append(current_type)
                new_type = st.selectbox(
                    "Type", type_options,
                    index=type_options.index(current_type),
                    key=f"type_{full_idx}",
                )

            cat_options = ["case", "specimen", "tissue", "blood", "whole_blood",
                           "plasma", "buffy_coat", "serum", "urine", "stool", "unmapped"]
            current_cat = m.get("category", "unmapped")
            if current_cat not in cat_options:
                cat_options.append(current_cat)

            col5, col6 = st.columns(2)
            with col5:
                new_cat = st.selectbox(
                    "Category", cat_options,
                    index=cat_options.index(current_cat),
                    key=f"cat_{full_idx}",
                )
            with col6:
                new_comments = st.text_input("Comments", value=m.get("comments") or "", key=f"cmt_{full_idx}")

            # Check for changes
            updated = {
                "csd": new_csd or None,
                "biomedica": new_bio or None,
                "output": new_output or None,
                "type": new_type,
                "category": new_cat,
                "comments": new_comments or None,
            }
            if updated != m:
                config["column_mappings"][full_idx] = updated
                edited = True

    # Add new mapping
    st.subheader("Add New Mapping")
    with st.form("new_mapping"):
        nc1, nc2, nc3, nc4 = st.columns(4)
        with nc1:
            new_m_csd = st.text_input("CSD column name")
        with nc2:
            new_m_bio = st.text_input("Biomedica column name")
        with nc3:
            new_m_out = st.text_input("Output field name")
        with nc4:
            new_m_type = st.selectbox("Field type", ["string", "integer", "float", "date", "time", "date/time", "boolean"])

        new_m_cat = st.selectbox("Category", ["case", "specimen", "tissue", "blood", "whole_blood",
                                               "plasma", "buffy_coat", "serum", "urine", "stool"])
        submitted = st.form_submit_button("Add Mapping")
        if submitted and (new_m_csd or new_m_bio or new_m_out):
            config["column_mappings"].append({
                "csd": new_m_csd or None,
                "biomedica": new_m_bio or None,
                "output": new_m_out or None,
                "type": new_m_type,
                "category": new_m_cat,
                "comments": None,
            })
            edited = True

    if edited:
        st.session_state.config = config
        save_config(config)
        st.success("Configuration saved!")


# ---------------------------------------------------------------------------
# Page: Export Templates
# ---------------------------------------------------------------------------

elif page == "Export Templates":
    st.title("Export Template Configuration")
    st.write("Configure which columns appear in each export file.")

    templates = config.get("export_templates", {})
    all_output_fields = sorted(set(
        m["output"] for m in config["column_mappings"] if m.get("output")
    ))

    edited = False
    for template_name, template in templates.items():
        with st.expander(f"{template_name}", expanded=False):
            current_cols = template.get("columns", [])

            st.write("**Current columns:**")
            # Allow reordering / removal via multiselect
            updated_cols = st.multiselect(
                "Columns (in order)",
                options=all_output_fields,
                default=[c for c in current_cols if c in all_output_fields],
                key=f"tmpl_{template_name}",
            )

            if updated_cols != current_cols:
                templates[template_name]["columns"] = updated_cols
                edited = True

    if edited:
        config["export_templates"] = templates
        st.session_state.config = config
        save_config(config)
        st.success("Export templates saved!")


# ---------------------------------------------------------------------------
# Page: Specimen ID Rules
# ---------------------------------------------------------------------------

elif page == "Specimen ID Rules":
    st.title("Specimen ID Generation Rules")
    st.write(
        "Configure how specimen IDs are generated from Donor ID for CSD manifests. "
        "Format: `<Donor ID> - <PREFIX>1, <PREFIX>2, ... <PREFIX>N`"
    )

    rules = config.get("specimen_id_rules", {})
    count_fields = sorted(set(
        m["output"] for m in config["column_mappings"]
        if m.get("output") and m.get("type") == "integer"
    ))

    edited = False
    for rule_name, rule in rules.items():
        with st.expander(f"{rule_name}", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                new_prefix = st.text_input("ID Prefix", value=rule.get("prefix", ""), key=f"pfx_{rule_name}")
            with col2:
                current_cf = rule.get("count_field") or ""
                cf_options = ["(none)"] + count_fields
                cf_idx = 0
                if current_cf in cf_options:
                    cf_idx = cf_options.index(current_cf)
                new_cf = st.selectbox(
                    "Count field", cf_options, index=cf_idx, key=f"cf_{rule_name}"
                )
                if new_cf == "(none)":
                    new_cf = None

            if new_prefix != rule.get("prefix") or new_cf != rule.get("count_field"):
                rules[rule_name] = {"prefix": new_prefix, "count_field": new_cf}
                edited = True

    # Add new rule
    st.subheader("Add New Specimen ID Rule")
    with st.form("new_id_rule"):
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            new_rule_name = st.text_input("Rule name (e.g. 'csf')")
        with rc2:
            new_rule_prefix = st.text_input("ID prefix (e.g. 'CSF')")
        with rc3:
            new_rule_cf = st.selectbox("Count field", ["(none)"] + count_fields)
        submitted = st.form_submit_button("Add Rule")
        if submitted and new_rule_name:
            rules[new_rule_name] = {
                "prefix": new_rule_prefix,
                "count_field": new_rule_cf if new_rule_cf != "(none)" else None,
            }
            edited = True

    if edited:
        config["specimen_id_rules"] = rules
        st.session_state.config = config
        save_config(config)
        st.success("Specimen ID rules saved!")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def df_to_excel_bytes(df):
    """Convert a DataFrame to Excel bytes for download."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    return buffer.getvalue()


def create_zip_download(cases_df, specimen_dfs):
    """Create a ZIP file containing all export Excel files."""
    import zipfile
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        if cases_df is not None and not cases_df.empty:
            zf.writestr("Cases.xlsx", df_to_excel_bytes(cases_df))
        for name, df in specimen_dfs.items():
            if df is not None and not df.empty:
                zf.writestr(f"{name}.xlsx", df_to_excel_bytes(df))
    return zip_buffer.getvalue()

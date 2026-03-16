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
    evaluate_alerts,
    CONFIG_PATH,
)

st.set_page_config(page_title="RM Manifest Processor", layout="wide")


# ---------------------------------------------------------------------------
# Utilities (must be defined before page code that calls them)
# ---------------------------------------------------------------------------

def render_cases_with_alerts(df, case_alerts):
    """Render a Cases DataFrame as an HTML table with alert rows highlighted in light red.

    Rows with alerts get a light-red background and a tooltip listing the active alerts.
    """
    import html as html_mod

    styles = """
    <style>
    .cases-table { border-collapse: collapse; width: 100%; font-size: 13px; }
    .cases-table th { background: #f0f2f6; padding: 6px 10px; border: 1px solid #ddd;
                       text-align: left; position: sticky; top: 0; }
    .cases-table td { padding: 6px 10px; border: 1px solid #ddd; white-space: nowrap; }
    .cases-table tr.alert-row { background-color: #ffe0e0; }
    .cases-table tr.alert-row:hover { background-color: #ffc8c8; }
    .cases-table tr:not(.alert-row):hover { background-color: #f5f5f5; }
    .alert-tooltip { position: relative; cursor: help; }
    .alert-tooltip .alert-text { visibility: hidden; background-color: #333; color: #fff;
        padding: 8px 12px; border-radius: 6px; position: absolute; z-index: 1000;
        bottom: 125%; left: 0; min-width: 280px; font-size: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3); }
    .alert-tooltip:hover .alert-text { visibility: visible; }
    </style>
    """

    header = "<tr>" + "".join(f"<th>{html_mod.escape(str(c))}</th>" for c in df.columns) + "</tr>"

    rows_html = []
    for idx, row in df.iterrows():
        alerts = case_alerts.get(idx, [])
        if alerts:
            tooltip_content = "<br>".join(html_mod.escape(a) for a in alerts)
            tooltip = (
                f'<span class="alert-text">'
                f'<strong>Alerts ({len(alerts)}):</strong><br>{tooltip_content}</span>'
            )
            # Wrap the first cell content with tooltip
            cells = []
            for i, col in enumerate(df.columns):
                val = html_mod.escape(str(row[col]) if pd.notna(row[col]) else "")
                if i == 0:
                    cells.append(f'<td class="alert-tooltip">{val}{tooltip}</td>')
                else:
                    cells.append(f"<td>{val}</td>")
            rows_html.append(f'<tr class="alert-row">{"".join(cells)}</tr>')
        else:
            cells = "".join(
                f"<td>{html_mod.escape(str(row[c]) if pd.notna(row[c]) else '')}</td>"
                for c in df.columns
            )
            rows_html.append(f"<tr>{cells}</tr>")

    table = f"""
    {styles}
    <div style="overflow-x: auto; max-height: 600px; overflow-y: auto;">
    <table class="cases-table">
    <thead>{header}</thead>
    <tbody>{"".join(rows_html)}</tbody>
    </table>
    </div>
    """
    return table


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
    if "case_alerts" not in st.session_state:
        st.session_state.case_alerts = {}
    if "cases_full_df" not in st.session_state:
        st.session_state.cases_full_df = None


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
    "Display As Rules",
    "Alert Rules",
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
            cases_df, specimen_dfs, wo_summary, warnings, unrecognized, case_alerts, cases_full_df = process_manifest(
                uploaded_files, supplier, config
            )

            st.session_state.cases_df = cases_df
            st.session_state.specimen_dfs = specimen_dfs
            st.session_state.wo_summary = wo_summary
            st.session_state.warnings = warnings
            st.session_state.unrecognized = unrecognized
            st.session_state.case_alerts = case_alerts
            st.session_state.cases_full_df = cases_full_df
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

        # Refresh Alerts button — re-evaluates alert rules against current cases
        if st.session_state.cases_full_df is not None and not st.session_state.cases_full_df.empty:
            if st.button("Refresh Alerts", help="Re-evaluate alert rules (use after adding/editing rules)"):
                fresh_config = load_config()
                st.session_state.config = fresh_config
                st.session_state.case_alerts = evaluate_alerts(st.session_state.cases_full_df, fresh_config)
                config = fresh_config
                st.rerun()

        # Alerts Summary Table
        case_alerts = st.session_state.case_alerts
        if case_alerts and st.session_state.cases_df is not None:
            cases_df = st.session_state.cases_df
            st.subheader("Alerts Summary")

            alert_rows = []
            for idx, alerts in case_alerts.items():
                donor = cases_df.at[idx, "Donor ID"] if "Donor ID" in cases_df.columns else f"Row {idx}"
                wo = cases_df.at[idx, "WO #"] if "WO #" in cases_df.columns else ""
                for alert_msg in alerts:
                    alert_rows.append({"WO #": wo, "Donor ID": donor, "Alert": alert_msg})

            alert_summary_df = pd.DataFrame(alert_rows)
            st.write(f"**{len(case_alerts)} case(s), {len(alert_rows)} total alert(s)**")
            st.dataframe(alert_summary_df, use_container_width=True, hide_index=True)

        # Cases preview & download (with alert highlighting)
        if st.session_state.cases_df is not None and not st.session_state.cases_df.empty:
            st.subheader("Cases Export")
            cases_df = st.session_state.cases_df
            case_alerts = st.session_state.case_alerts

            if case_alerts:
                st.markdown(render_cases_with_alerts(cases_df, case_alerts), unsafe_allow_html=True)
            else:
                st.dataframe(cases_df, use_container_width=True, hide_index=True)

            st.download_button(
                "Download Cases.xlsx",
                data=df_to_excel_bytes(cases_df),
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
# Page: Display As Rules
# ---------------------------------------------------------------------------

elif page == "Display As Rules":
    st.title("Display As Rules")
    st.write(
        'Substitute cell values before export. For example, display "M" as "Male". '
        "Rules are applied after mapping and calculations."
    )

    all_output_fields = sorted(set(
        m["output"] for m in config["column_mappings"] if m.get("output")
    ))

    display_rules = config.get("display_as_rules", [])

    edited = False

    for i, rule in enumerate(display_rules):
        with st.expander(
            f'"{rule.get("match", "")}" → "{rule.get("display_as", "")}" '
            f'({len(rule.get("columns", []))} columns)',
            expanded=False,
        ):
            col1, col2 = st.columns(2)
            with col1:
                new_match = st.text_input("Match phrase", value=rule.get("match", ""), key=f"da_match_{i}")
            with col2:
                new_display = st.text_input("Display as", value=rule.get("display_as", ""), key=f"da_disp_{i}")

            new_cols = st.multiselect(
                "Apply to columns",
                options=all_output_fields,
                default=[c for c in rule.get("columns", []) if c in all_output_fields],
                key=f"da_cols_{i}",
            )

            new_exact = st.checkbox(
                "Exact match (entire cell must equal the phrase)",
                value=rule.get("exact_match", True),
                key=f"da_exact_{i}",
            )

            col_del, _ = st.columns([1, 3])
            with col_del:
                if st.button("Delete this rule", key=f"da_del_{i}"):
                    display_rules.pop(i)
                    edited = True
                    st.rerun()

            updated_rule = {
                "match": new_match,
                "display_as": new_display,
                "columns": new_cols,
                "exact_match": new_exact,
            }
            if updated_rule != rule:
                display_rules[i] = updated_rule
                edited = True

    # Add new rule
    st.subheader("Add New Rule")
    with st.form("new_display_as_rule"):
        nc1, nc2 = st.columns(2)
        with nc1:
            new_r_match = st.text_input("Match phrase (e.g. 'M')")
        with nc2:
            new_r_display = st.text_input("Display as (e.g. 'Male')")

        new_r_cols = st.multiselect("Apply to columns", options=all_output_fields)
        new_r_exact = st.checkbox("Exact match (entire cell must equal the phrase)", value=True)

        submitted = st.form_submit_button("Add Rule")
        if submitted and new_r_match:
            display_rules.append({
                "match": new_r_match,
                "display_as": new_r_display,
                "columns": new_r_cols,
                "exact_match": new_r_exact,
            })
            edited = True

    if edited:
        config["display_as_rules"] = display_rules
        st.session_state.config = config
        save_config(config)
        st.success("Display As rules saved!")


# ---------------------------------------------------------------------------
# Page: Alert Rules
# ---------------------------------------------------------------------------

elif page == "Alert Rules":
    st.title("Alert Rules")
    st.write(
        "Configure alerts that fire when case data meets certain conditions. "
        "Cases with active alerts are highlighted in red on the Process Manifests page."
    )

    all_output_fields = sorted(set(
        m["output"] for m in config["column_mappings"] if m.get("output")
    ))

    condition_types = [
        "value_equals",
        "value_contains",
        "is_empty",
        "is_not_empty",
        "greater_than",
        "less_than",
        "is_negative",
        "column_before",
        "column_after",
        "column_equals",
        "column_not_equals",
        "column_greater_than",
        "column_less_than",
    ]
    condition_labels = {
        "value_equals": "Value equals",
        "value_contains": "Value contains",
        "is_empty": "Is empty/blank",
        "is_not_empty": "Is not empty",
        "greater_than": "Greater than (numeric)",
        "less_than": "Less than (numeric)",
        "is_negative": "Is negative (numeric)",
        "column_before": "Is before (date/time) another column",
        "column_after": "Is after (date/time) another column",
        "column_equals": "Equals another column",
        "column_not_equals": "Does not equal another column",
        "column_greater_than": "Is greater than another column (numeric)",
        "column_less_than": "Is less than another column (numeric)",
    }
    needs_value = {"value_equals", "value_contains", "greater_than", "less_than"}
    needs_compare_column = {
        "column_before", "column_after", "column_equals",
        "column_not_equals", "column_greater_than", "column_less_than",
    }

    alert_rules = config.get("alert_rules", [])

    edited = False

    for i, rule in enumerate(alert_rules):
        cond_type = rule.get("condition_type", "")
        cond_label = condition_labels.get(cond_type, cond_type)
        compare_col = rule.get("compare_column", "")
        if cond_type in needs_compare_column and compare_col:
            expander_label = f'{rule.get("message", "Alert")} — {rule.get("column", "?")} {cond_label} {compare_col}'
        else:
            expander_label = f'{rule.get("message", "Alert")} — {rule.get("column", "?")} {cond_label}'

        with st.expander(expander_label, expanded=False):
            new_msg = st.text_input("Alert message", value=rule.get("message", ""), key=f"al_msg_{i}")

            col1, col2 = st.columns(2)
            with col1:
                current_col = rule.get("column", "")
                col_options = [""] + all_output_fields
                col_idx = col_options.index(current_col) if current_col in col_options else 0
                new_col = st.selectbox("Column", col_options, index=col_idx, key=f"al_col_{i}")

            with col2:
                current_cond = rule.get("condition_type", "value_equals")
                cond_idx = condition_types.index(current_cond) if current_cond in condition_types else 0
                new_cond = st.selectbox(
                    "Condition",
                    condition_types,
                    index=cond_idx,
                    format_func=lambda x: condition_labels.get(x, x),
                    key=f"al_cond_{i}",
                )

            new_val = ""
            new_compare_col = ""
            if new_cond in needs_value:
                new_val = st.text_input("Value", value=str(rule.get("value", "")), key=f"al_val_{i}")
            elif new_cond in needs_compare_column:
                current_cc = rule.get("compare_column", "")
                cc_options = [""] + all_output_fields
                cc_idx = cc_options.index(current_cc) if current_cc in cc_options else 0
                new_compare_col = st.selectbox(
                    "Compare to column", cc_options, index=cc_idx, key=f"al_cc_{i}"
                )

            col_del, _ = st.columns([1, 3])
            with col_del:
                if st.button("Delete this rule", key=f"al_del_{i}"):
                    alert_rules.pop(i)
                    edited = True
                    st.rerun()

            updated_rule = {
                "message": new_msg,
                "column": new_col,
                "condition_type": new_cond,
                "value": new_val,
                "compare_column": new_compare_col,
            }
            if updated_rule != rule:
                alert_rules[i] = updated_rule
                edited = True

    # Add new rule
    st.subheader("Add New Alert Rule")
    with st.form("new_alert_rule"):
        new_a_msg = st.text_input("Alert message (shown on hover)")

        ac1, ac2 = st.columns(2)
        with ac1:
            new_a_col = st.selectbox("Column to check", [""] + all_output_fields)
        with ac2:
            new_a_cond = st.selectbox(
                "Condition type",
                condition_types,
                format_func=lambda x: condition_labels.get(x, x),
            )

        new_a_val = ""
        new_a_cc = ""
        if new_a_cond in needs_value:
            new_a_val = st.text_input("Value (for equals/contains/greater/less conditions)")
        elif new_a_cond in needs_compare_column:
            new_a_cc = st.selectbox("Compare to column", [""] + all_output_fields, key="new_al_cc")

        submitted = st.form_submit_button("Add Alert Rule")
        if submitted and new_a_msg and new_a_col:
            alert_rules.append({
                "message": new_a_msg,
                "column": new_a_col,
                "condition_type": new_a_cond,
                "value": new_a_val,
                "compare_column": new_a_cc,
            })
            edited = True

    if edited:
        config["alert_rules"] = alert_rules
        st.session_state.config = config
        save_config(config)
        st.success("Alert rules saved!")

"""This module contains helper functions."""

import logging
import json
import urllib.parse

import pandas as pd

from sqlalchemy import create_engine

from sub_processes import formular_mappings

logger = logging.getLogger(__name__)


def get_forms_data(
    conn_string: str,
    form_type: str,
    target_date: str = "",
    start_date: str = "",
    end_date: str = ""
) -> list[dict]:
    """
    Retrieve form_data['data'] for all matching submissions for the given form type.
    Supports either:
      - exact date (target_date)
      - date range (start_date + end_date)
      - no date filter (all submissions for form_type)
    Skips entries marked as purged.
    """

    where_clause = ""

    # Build query depending on which filter type is used
    if start_date and end_date:
        where_clause = "AND CAST(form_submitted_date AS date) BETWEEN ? AND ?"

        query_params = (form_type, start_date, end_date)

    elif target_date:
        where_clause = "AND CAST(form_submitted_date AS date) = ?"

        query_params = (form_type, target_date)

    else:
        query_params = (form_type,)

    query = f"""
        SELECT
            form_id,
            form_data,
            CAST(form_submitted_date AS datetime) AS form_submitted_date
        FROM
            [RPA].[journalizing].[Forms]
        WHERE
            form_type = ?
            AND form_data IS NOT NULL
            AND form_submitted_date IS NOT NULL
            {where_clause}
        ORDER BY
            form_submitted_date DESC
    """

    logger.info(f"Executing SQL query:\n{query}")

    # Create SQLAlchemy engine
    encoded_conn_str = urllib.parse.quote_plus(conn_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}")

    try:
        df = pd.read_sql(sql=query, con=engine, params=query_params)

    except Exception as e:
        logger.info(f"Error during pd.read_sql: {e}")

        raise

    if df.empty:
        logger.info("No submissions found for the given date(s).")

        return []

    extracted_data = []

    for _, row in df.iterrows():
        try:
            parsed = json.loads(row["form_data"])

            if "purged" not in parsed:
                extracted_data.append(parsed)

        except json.JSONDecodeError:
            logger.info("Invalid JSON in form_data, skipping row.")

    return extracted_data


def build_df(submissions, role, mapping):
    """
    Build a DataFrame from the given submissions and mapping for the specified role.
    The role determines which mapping to use and which submissions to include.
    """

    rows = []

    for submission in submissions:
        if submission["data"].get("hvem_udfylder_spoergeskemaet") != role:
            continue

        serial = submission["entity"]["serial"][0]["value"]

        rows.append(formular_mappings.transform_form_submission(serial, submission, mapping))

    return pd.DataFrame(rows)


def format_html_table(table_att: dict) -> str:
    """
    Create an HTML table from a dictionary of attributes.
    """

    html = '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">\n'

    for key, value in table_att.items():
        html += f'  <tr><td><strong>{key}</strong></td><td>{value}</td></tr>\n'

    html += '</table>'

    return html

"""
Script to perform the monthly update of the Excel files with new submissions.
"""

from io import BytesIO

import pandas as pd

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from sub_processes import helper_functions
from sub_processes import formular_mappings

SHEET_NAME = "Besvarelser"


async def montly_update_excel_file(sharepoint_api: Sharepoint, db_conn_string: str, os2_webform_id: str, folder_name: str):
    """Perform the monthly update of the Excel files with new submissions."""

    date_today = pd.Timestamp.now().date()

    current_day_of_month = str(pd.Timestamp.now().day)
    if current_day_of_month == "1":
        print("Today is the first of the month - we will update the Excel files with new submissions.")

        # Last + first day of last month
        end_date = date_today.replace(day=1) - pd.Timedelta(days=1)
        start_date = end_date.replace(day=1)

        unge_excel_file_name = "Center for trivsel - ESQ besvarelser fra unge.xlsx"
        foraeldre_excel_file_name = "Center for trivsel - ESQ besvarelser fra forældre.xlsx"

        files_in_sharepoint = sharepoint_api.fetch_files_list(folder_name=folder_name)
        file_names = [f["Name"] for f in files_in_sharepoint]

        for excel_file_name in [unge_excel_file_name, foraeldre_excel_file_name]:
            if excel_file_name not in file_names:
                print(f"Excel file '{excel_file_name}' not found - creating new.")

                # Fetch all submissions once for the whole period
                all_submissions = helper_functions.get_forms_data(db_conn_string, os2_webform_id)

                if excel_file_name == unge_excel_file_name:
                    all_submissions_df = helper_functions.build_df(all_submissions, "Ung/selvbesvarelse", formular_mappings.center_for_trivsel_esq_barn_mapping)

                else:
                    all_submissions_df = helper_functions.build_df(all_submissions, "Forælder (inklusiv plejeforældre)", formular_mappings.center_for_trivsel_esq_foraelder_mapping)

                excel_stream = BytesIO()
                all_submissions_df.to_excel(excel_stream, index=False, engine="openpyxl", sheet_name=SHEET_NAME)
                excel_stream.seek(0)

                sharepoint_api.upload_file_from_bytes(
                    binary_content=excel_stream.getvalue(),
                    file_name=excel_file_name,
                    folder_name=folder_name
                )

            else:
                print(f"Fetching forms from {start_date} to {end_date} for '{excel_file_name}'.")

                ranged_submissions = helper_functions.get_forms_data(
                    db_conn_string,
                    os2_webform_id,
                    start_date=start_date,
                    end_date=end_date
                )

                # Filter/transform for just this file
                if excel_file_name == unge_excel_file_name:
                    new_rows_df = helper_functions.build_df(ranged_submissions, "Ung/selvbesvarelse", formular_mappings.center_for_trivsel_esq_barn_mapping)

                else:
                    new_rows_df = helper_functions.build_df(ranged_submissions, "Forælder (inklusiv plejeforældre)", formular_mappings.center_for_trivsel_esq_foraelder_mapping)

                if not new_rows_df.empty:
                    sharepoint_api.append_row_to_sharepoint_excel(
                        folder_name=folder_name,
                        excel_file_name=excel_file_name,
                        sheet_name=SHEET_NAME,
                        new_rows=new_rows_df.to_dict(orient="records")
                    )

            # Format after create/append
            sharepoint_api.format_and_sort_excel_file(
                folder_name=folder_name,
                excel_file_name=excel_file_name,
                sheet_name=SHEET_NAME,
                sorting_keys=[{"key": "A", "ascending": False, "type": "str"}],
                bold_rows=[1],
                align_horizontal="left",
                align_vertical="top",
                italic_rows=None,
                font_config=None,
                column_widths=100,
                freeze_panes="A2"
            )

            print()
            print()

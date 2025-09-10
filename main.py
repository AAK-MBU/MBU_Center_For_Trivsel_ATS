"""
main.py
"""

import sys
import os

import traceback
import asyncio

import datetime

from io import BytesIO

import pandas as pd

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue, WorkItemError

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint
from mbu_dev_shared_components.database.connection import RPAConnection

from sub_processes import helper_functions
from sub_processes import formular_mappings
from sub_processes import smtp_util
from sub_processes.montly_excel_update import montly_update_excel_file


### REMOVE IN PRODUCTION ###
# import requests
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# _old_request = requests.Session.request


# def unsafe_request(self, *args, **kwargs):
#     """
#     TESTING PURPOSES ONLY - DISABLES SSL VERIFICATION FOR ALL REQUESTS
#     """
#     kwargs['verify'] = False
#     return _old_request(self, *args, **kwargs)


# requests.Session.request = unsafe_request
### REMOVE IN PRODUCTION ###


load_dotenv()

ATS_URL = os.getenv("ATS_URL")
ATS_TOKEN = os.getenv("ATS_TOKEN")

DB_CONN_STRING = os.getenv("DBConnectionStringProd")
# DB_CONN_STRING = os.getenv("DbConnectionStringDev")  # UNCOMMENT FOR DEV TESTING

# TEMPORARY OVERRIDE: Set a new env variable in memory only
os.environ["DbConnectionString"] = os.getenv("DBConnectionStringProd")

RPA_CONN = RPAConnection(db_env="PROD", commit=False)
with RPA_CONN:
    SCV_LOGIN = RPA_CONN.get_credential("SvcRpaMbu002")
    USERNAME = SCV_LOGIN.get("username", "")
    PASSWORD = SCV_LOGIN.get("decrypted_password", "")

    RPA_EMAIL_NO_REPLY = RPA_CONN.get_constant("e-mail_noreply").get("value", "")

    SMTP_SERVER = RPA_CONN.get_constant("smtp_server").get("value", "")
    SMTP_PORT = RPA_CONN.get_constant("smtp_port").get("value", "")

    # CENTER_FOR_TRIVSEL_MAIL = RPA_CONN.get_constant("center_for_trivsel_mail")

SHAREPOINT_FOLDER_URL = "https://aarhuskommune.sharepoint.com"

SHAREPOINT_API = Sharepoint(
    username=USERNAME,
    password=PASSWORD,
    site_url="https://aarhuskommune.sharepoint.com",
    site_name="CenterforTrivsel",
    document_library="Delte dokumenter"
)

OS2_WEBFORM_ID = "center_for_trivsel_esq_formular"

FOLDER_NAME = "General/ESQ"


async def populate_queue(workqueue: Workqueue):
    """Populate the workqueue with items to be processed."""

    # ALWAYS RUN DAILY EMAIL SUBMISSION FLOW
    print("Running daily email submission flow.")

    forms_by_cpr = {}

    date_yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).date()
    all_yesterdays_forms = helper_functions.get_forms_data(DB_CONN_STRING, OS2_WEBFORM_ID, target_date=date_yesterday)

    ### UNCOMMENT IN PRODUCTION ###
    # approved_emails_bytes = SHAREPOINT_API.fetch_file_using_open_binary(
    #     file_name="Godkendte emails.xlsx",
    #     folder_name=FOLDER_NAME
    # )

    # approved_emails_df = pd.read_excel(BytesIO(approved_emails_bytes))

    # # Create dictionary {az-ident: email}, dropping NaNs and stripping/normalizing
    # approved_emails_dict = dict(
    #     zip(
    #         approved_emails_df['az-ident'].dropna().str.strip(),
    #         approved_emails_df['email'].dropna().str.strip().str.lower()
    #     )
    # )
    ### UNCOMMENT IN PRODUCTION ###

    if len(all_yesterdays_forms) > 0:
        for form in all_yesterdays_forms:
            try:
                serial = form["entity"]["serial"][0]["value"]

                udfylder_rolle = form["data"]["hvem_udfylder_spoergeskemaet"]

                if udfylder_rolle == "Ung/selvbesvarelse":
                    mapping = formular_mappings.center_for_trivsel_esq_barn_mapping

                elif udfylder_rolle == "Forælder (inklusiv plejeforældre)":
                    mapping = formular_mappings.center_for_trivsel_esq_foraelder_mapping

                else:
                    continue

                transformed_row = formular_mappings.transform_form_submission(serial, form, mapping)

                ### UNCOMMENT IN PRODUCTION ###
                # if transformed_row["AZ-ident"].strip() not in approved_emails_dict:
                #     transformed_row["Tilkoblet email"] = CENTER_FOR_TRIVSEL_MAIL

                # else:
                #     transformed_row["Tilkoblet email"] = approved_emails_dict[transformed_row["AZ-ident"].strip().lower()]
                ### UNCOMMENT IN PRODUCTION ###

                transformed_row["Tilkoblet email"] = "dadj@aarhus.dk"  # REMOVE IN PRODUCTION

                cpr = transformed_row["Barnets/Den unges CPR-nummer"]

                if cpr not in forms_by_cpr:
                    forms_by_cpr[cpr] = []

                forms_by_cpr[cpr].append({
                    "form": form,
                    "transformed": transformed_row,
                    "role": udfylder_rolle,
                    "mapping": mapping
                })

            except Exception as e:
                print(f"Error processing form: {e}")

                continue

        for cpr, submissions in forms_by_cpr.items():
            sections = []

            for entry in submissions:
                transformed_row = entry["transformed"]
                role = entry["role"]
                mapping = entry["mapping"]

                table_att = {
                    "Udfyldt": transformed_row["Gennemført"],
                    "Behandling": transformed_row["Behandling"],
                    "Barnets/Den unges navn": transformed_row["Barnets/Den unges navn"],
                    "Barnets/Den unges CPR-nummer": transformed_row["Barnets/Den unges CPR-nummer"],
                    "Barnets/Den unges alder": transformed_row["Barnets/Den unges alder"],
                }

                if role == "Forælder (inklusiv plejeforældre)":
                    table_att["Forælder navn"] = transformed_row["Navn"]
                    table_att["Forælder cpr-Nummer"] = transformed_row["CPR-nummer"]

                    for _, spg in mapping["spoergsmaal_foraelder_tabel"].items():
                        table_att[spg] = transformed_row.get(spg)

                    table_att["Hvad var rigtig godt ved behandlingen?"] = transformed_row["Hvad var rigtig godt ved behandlingen?"]
                    table_att["Var der noget du ikke synes om eller noget der kan forbedres?"] = transformed_row["Var der noget du ikke synes om eller noget der kan forbedres?"]
                    table_att["Er der andet du ønsker at fortælle os, om det forløb I har haft?"] = transformed_row["Er der andet du ønsker at fortælle os, om det forløb I har haft?"]

                else:
                    for _, spg in mapping["spoergsmaal_barn_tabel"].items():
                        table_att[spg] = transformed_row.get(spg)

                    table_att["Her er plads til, at du kan skrive, hvad du tænker eller føler om behandlingen"] = transformed_row["Her er plads til, at du kan skrive, hvad du tænker eller føler om behandlingen"]

                table_att["Average answer score"] = transformed_row["Average answer score"]

                html_table = helper_functions.format_html_table(table_att)

                sections.append(
                    f"<p><strong>Udfylder rolle:</strong> {role}</p><br>{html_table}<br><br>"
                )

            email_body = (
                f"<p>Ny(e) besvarelse(r) til ESQ formular for barn med CPR: <strong>{cpr}</strong></p>"
                + "<hr>".join(sections)
            )

            data = {
                "email_receiver": transformed_row["Tilkoblet email"],
                "email_body": email_body,
            }

            workqueue.add_item(
                data=data,
                reference=cpr
            )


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    print("Processing workqueue items...")

    for item in workqueue:
        with item:
            reference = item.reference

            data = item.data

            print(f"Processing item with reference: {reference}")

            email_receiver = data.get("email_receiver", "")

            email_body = data.get("email_body", "")

            try:
                smtp_util.send_email(
                    receiver=email_receiver,
                    sender=RPA_EMAIL_NO_REPLY,
                    subject="Ny(e) ESQ besvarelse(r)",
                    body=email_body,
                    html_body=email_body,
                    smtp_server=SMTP_SERVER,
                    smtp_port=SMTP_PORT,
                    attachments=None,
                )

            except WorkItemError as e:
                print(f"Error processing item: {data}. Error: {e}")

                item.fail(str(e))

                traceback.print_exc()

            # except Exception as e:
            #     print("❌ Failed to send email")

            #     print(f"➡️ Error: {e}")

            #     traceback.print_exc()


if __name__ == "__main__":
    ats = AutomationServer.from_environment()

    center_for_trivsel_workqueue = ats.workqueue()

    print(f"Workqueue: {center_for_trivsel_workqueue}\n")

    if "--queue" in sys.argv:
        asyncio.run(populate_queue(center_for_trivsel_workqueue))

        if datetime.date.today() == 1 or "--monthly-update" in sys.argv:
            asyncio.run(montly_update_excel_file(sharepoint_api=SHAREPOINT_API, db_conn_string=DB_CONN_STRING, os2_webform_id=OS2_WEBFORM_ID, folder_name=FOLDER_NAME))

            print("Monthly update triggered (by date or flag).")

        sys.exit()

    else:
        asyncio.run(process_workqueue(center_for_trivsel_workqueue))

"""
file to contain formular mappings

Ideally we wouldn't have to hardcode the mappings, but we the column names from the API are inconsistent in spelling and casing - therefore we need to map them to the correct column names in the Excel file.
"""

import ast

from datetime import datetime

center_for_trivsel_esq_barn_mapping = {
    "serial": "Serial number",
    "created": "Oprettet",
    "completed": "Gennemført",
    "az": "AZ-ident",
    "hvem_udfylder_spoergeskemaet": "Hvem udfylder spørgeskemaet",
    "navn_manuelt": "Barnets/Den unges navn",
    "cpr_nummer_manuelt": "Barnets/Den unges CPR-nummer",
    "beregnet_alder": "Barnets/Den unges alder",
    "behandling": "Behandling",
    "spoergsmaal_barn_tabel": {
        "spg_barn_1": 'Behandlingen hjalp mig',
        "spg_barn_2": 'Vi har det bedre i familien nu, end før behandlingen begyndte',
        "spg_barn_3": 'Hvis en ven havde brug for denne form for hjælp, ville jeg anbefale ham/hende at komme på klinikken',
        "spg_barn_4": 'Behandlerne forstod det vigtigste af mine bekymringer og problemer',
        "spg_barn_5": 'Jeg havde tillid til behandleren',
        "spg_barn_6": 'Behandlingen medførte, at jeg fik det dårligere',
        "spg_barn_7": 'Efter behandlingen har jeg fået mere lyst til at være sammen med mine venner',
    },
    "her_er_plads_til_at_du_kan_skrive_hvad_du_taenker_eller_foeler_o": "Her er plads til, at du kan skrive, hvad du tænker eller føler om behandlingen",
}

center_for_trivsel_esq_foraelder_mapping = {
    "serial": "Serial number",
    "created": "Oprettet",
    "completed": "Gennemført",
    "az": "AZ-ident",
    "hvem_udfylder_spoergeskemaet": "Hvem udfylder spørgeskemaet",
    "navn_manuelt": "Navn",
    "cpr_nummer_manuelt": "CPR-nummer",
    "barnets_navn_manuelt": "Barnets/Den unges navn",
    "cpr_nummer_barnet_manuelt": "Barnets/Den unges CPR-nummer",
    "beregnet_alder": "Barnets/Den unges alder",
    "behandling": "Behandling",
    "spoergsmaal_foraelder_tabel": {
        "spg_foraelder_1": 'Behandlingen hjalp mit barn',
        "spg_foraelder_2": 'Behandlingen hjalp mig',
        "spg_foraelder_3": 'Hvis en ven havde brug for denne form for hjælp, ville jeg anbefale vedkommende at komme på klinikken',
        "spg_foraelder_4": 'Jeg følte mig passende informeret om meningen, formålet og forløbet af behandlingen',
        "spg_foraelder_5": 'Vi har det bedre i familien nu, end før behandlingen begyndte',
        "spg_foraelder_6": 'Under forløbet af behandlingen blev jeg i stand til at forandre min adfærd over for mit barn på en positiv måde',
        "spg_foraelder_7": 'Under forløbet af behandlingen opnåede jeg en bedre forståelse af mit barns psykiske tilstand',
        "spg_foraelder_8": 'Jeg havde tillid til vores behandlere',
        "spg_foraelder_9": 'Behandlingen medførte, at mit barn fik det dårligere',
        "spg_foraelder_10": 'Behandlingen medførte, at jeg fik det dårligere',
    },
    "hvad_var_rigtig_godt_ved_forloebet": "Hvad var rigtig godt ved behandlingen?",
    "var_der_noget_du_ikke_syntes_om_eller_noget_der_kan_forbedres": "Var der noget du ikke synes om eller noget der kan forbedres?",
    "er_der_andet_du_oensker_at_fortaelle_os_om_det_forloeb_du_har_haft": "Er der andet du ønsker at fortælle os, om det forløb I har haft?",
}


def transform_form_submission(form_serial_number, form: dict, mapping: dict) -> dict:
    """
    Transforms a form submission dictionary using the provided mapping.
    Adds 'Average answer score' based on responses.
    """

    transformed = {}
    form_data = form.get("data", {})

    # For scoring
    total_score = 0
    score_count = 0

    # Identify inverted keys per mapping
    inverted_keys = set()
    if mapping is center_for_trivsel_esq_barn_mapping:
        inverted_keys = {"spg_barn_6"}
    elif mapping is center_for_trivsel_esq_foraelder_mapping:
        inverted_keys = {"spg_foraelder_9", "spg_foraelder_10"}

    answer_scores = {
        "Ikke sandt": 0,
        "Delvist sandt": 1,
        "Sandt": 2,
    }

    inverted_answer_scores = {
        "Ikke sandt": 0,
        "Delvist sandt": -1,
        "Sandt": -2,
    }

    for source_key, target in mapping.items():
        if isinstance(target, dict):  # Handle nested mapping like spoergsmaal_barn_tabel
            nested_data = form_data.get(source_key, {})

            if not isinstance(nested_data, dict):
                raise TypeError(
                    f"Expected nested data for '{source_key}' to be a dict, but got {type(nested_data).__name__}"
                )

            for nested_key, nested_target_column in target.items():
                value = nested_data.get(nested_key, None)

                # Convert answers to scores
                if value in answer_scores:
                    if nested_key in inverted_keys:
                        total_score += inverted_answer_scores[value]

                    else:
                        total_score += answer_scores[value]

                    score_count += 1

                # Standard formatting logic
                if isinstance(value, list):
                    value = ", ".join(str(item) for item in value)

                elif isinstance(value, str):
                    value = value.replace("\r\n", ". ").replace("\n", ". ")

                    if value.startswith("[") and value.endswith("]"):
                        try:
                            parsed = ast.literal_eval(value)

                            if isinstance(parsed, list):
                                value = ", ".join(str(item) for item in parsed)

                        except Exception:
                            value = value.strip("[]").replace("'", "").replace('"', "").strip()

                transformed[nested_target_column] = value

        else:  # Handle flat fields
            value = form_data.get(source_key, None)

            if isinstance(value, list):
                value = ", ".join(str(item) for item in value)

            elif isinstance(value, str):
                value = value.replace("\r\n", ". ").replace("\n", ". ")

                if value.startswith("[") and value.endswith("]"):
                    try:
                        parsed = ast.literal_eval(value)

                        if isinstance(parsed, list):
                            value = ", ".join(str(item) for item in parsed)

                    except Exception:
                        value = value.strip("[]").replace("'", "").replace('"', "").strip()

            transformed[target] = value

    # Dates from "entity"
    try:
        created_str = form["entity"]["created"][0]["value"]
        completed_str = form["entity"]["completed"][0]["value"]
        created_dt = datetime.fromisoformat(created_str)
        completed_dt = datetime.fromisoformat(completed_str)
        transformed["Oprettet"] = created_dt.strftime("%Y-%m-%d %H:%M:%S")
        transformed["Gennemført"] = completed_dt.strftime("%Y-%m-%d %H:%M:%S")

    except (KeyError, IndexError, ValueError):
        transformed["Oprettet"] = None
        transformed["Gennemført"] = None

    transformed["Serial number"] = form_serial_number

    # ✅ Add average answer score
    transformed["Average answer score"] = round(total_score / score_count, 2) if score_count else None

    return transformed

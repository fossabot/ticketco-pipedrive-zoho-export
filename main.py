# -*- coding: utf-8 -*-

import os
import json
from google.cloud import storage
import requests as re
from datetime import datetime
from time import sleep
import pandas as pd


import collections

def main(data, context):

    # Get API key
    print("API-key for Pipedrive is stored in pipedrive-config file")
    pdkey, pduser, pdpwd = config_values()["pd"].split(",")
    slack = config_values()["slack"]

    ### ORGS
    org_options, org_keys = get_organization_fields(pdkey)
    # input('Continue?')

    ### Get org data

    pd_orgs = org_content(pdkey, org_keys, org_options)

    ################# START ##################
    url = (
        "https://hooks.slack.com/services/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"
    )
    logger = re.post(url, json={"text": "\n\nScript started @" + str(datetime.now())})

    # GET the fields
    id_key_name, all_options, stager, piper = get_deal_fields(pdkey)

    # Get the deals
    content = get_all_deals_api(pdkey)

    ## Fix the fieldnames, and make it one to one
    final_deals_data = {}

    for listers in content:
        for lister in listers:

            deal = dict(lister)
            dealnew = {}
            for key, value in deal.items():

                if all_options.get(id_key_name.get(key)) == None:
                    pass
                else:
                    if value == None:
                        pass
                    else:
                        if key == "lost_reason":
                            pass
                        else:
                            deal[key] = all_options[key][value]
                if key == "pipeline_id":
                    deal[key] = piper[value]
                if key == "stage_id":
                    deal[key] = stager[value]

                if id_key_name.get(key) == None:
                    dealnew[key] = deal[key]
                else:
                    dealnew[id_key_name[key]] = deal[key]

            dealnew = flatten(dealnew)

            final_deals_data[deal["id"]] = dealnew

    ############### UPDATE ZOHO! #######################
    pd_deals = pd.DataFrame.from_dict(final_deals_data, orient="index")
    pd_deals.rename(columns={"Organization_value": "Deals_id"}, inplace=True)
    pd_orgs.rename(columns={"ID": "Deals_id"}, inplace=True)
    print(f"DEALS: {pd_deals.columns.values}")
    print(f"ORGS: {pd_orgs.columns.values}")
    # input("check")
    # print(pd_deals['company_id'])
    # print(pd_orgs['company_id'])
    # input('c')
    ########### Merge #################3
    deals2 = pd_deals.merge(
        pd_orgs,
        how="left",
        on="Deals_id",
        left_on=None,
        right_on=None,
        left_index=False,
        right_index=False,
        sort=True,
        suffixes=("_deals", "_org"),
        copy=True,
        indicator=False,
    )

    print(deals2.info)

    # input("T2")
    csvdata = deals2.to_csv()
    # pd_deals.to_csv("deals.csv", sep=";")
    csvdata = "index" + csvdata

    log_za = update_ZA_deals_data(config_values()["za"], csvdata)

    ##### Tell the world #####
    log_za = eval(log_za)
    logg = "Done, but there might be errors, investigate."
    color = "#000000"
    print(log_za)
    if log_za["response"].get("result") != None:
        logg = (
            str(log_za["response"]["result"]["importSummary"])
            .replace('"', "")
            .replace(",", "\n")
            .replace("'", "'")
            + "\n"
            + str(log_za["response"]["result"]["importErrors"])
            .replace('"', "")
            .replace(",", "\n")
        )
        color = "#36a64f"
        warnings = str(log_za["response"]["result"]["importSummary"]["warnings"])
        pretext = (
            "Script finished successfully @ "
            + str(datetime.now())
            + " with "
            + warnings
            + " warnings"
        )
    if log_za["response"].get("error") != None:
        logg = str(log_za["response"]["error"]["message"])
        color = "#ff0000"
        pretext = "<@channel> We have a *PROBLEM* - please help me:\n"

    ### report to slack    #
    url = slack
    logger = re.post(
        url,
        json={
            "attachments": [
                {"pretext": pretext, "color": color, "text": "\nLOG: \n" + logg}
            ]
        },
    )
    print(logger.text)
    print("\n****************\n*     DONE     *\n****************\n")


def flatten(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        # print(f'Key: {k}')
        # print(f'Value: {v}')
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(
            v, list
        ):  # Removes lists and takes first item in list as the value
            v = v[0]
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def config_values():
    content = storage.Client() \
        .get_bucket(os.environ['STORAGE_BUCKET']) \
        .get_blob('pipedrive-config') \
        .download_as_string()

    #with open("pipedrive-config", "r", encoding="utf-8") as f:
    #    content = f.read()
    # dater=arrow.now().format('YYYYMMDD')
    return eval(content)


def get_organization_fields(apikey):
    # dealfields
    url = (
        "https://ticketco.pipedrive.com/v1/organizationFields/?limit=500&api_token="
        + apikey
    )
    content1 = re.get(url)
    print(content1.text)

    id_key_name = {}

    all_options = {}
    for field in content1.json()["data"]:
        # key, value in field.items()
        # print(field)

        ## GET OPTIONS
        option = field.get("options")
        if option == None:
            pass
        else:
            all_options_specific = {}
            for dicter in option:
                # Todo Add check if the key exists already
                all_options_specific[str(dicter["id"])] = dicter["label"]
            all_options[field.get("key")] = all_options_specific

        ## GET ID, KEY, and NAME
        id = field["id"]
        keyer = field["key"]
        name = field["name"]
        id_key_name[keyer] = name

        all_options_new = {}
        for key, value in all_options.items():
            all_options_new[id_key_name[key]] = value
            all_options_new[key] = value
    print("\n\n" + str(all_options_new))
    print("\n\n" + str(id_key_name))
    return all_options_new, id_key_name


def org_content(apikey, orgfields, options):
    url = (
        "https://ticketco.pipedrive.com/v1/organizations/?limit=500&api_token=" + apikey
    )
    content1 = []
    page = 0
    while True:
        print(f"Page: {page}")
        url = (
            "https://ticketco.pipedrive.com/v1/organizations/?limit=500&api_token="
            + apikey
            + "&start="
            + str(page)
        )
        data = re.get(url)
        # print(data.status_code)
        # print(data.json()['data'])
        if (data.json()["data"]) == None:
            print(f"** Breaking @ {page} **")
            break
        # print(data.text)
        # print(data.json()['data'][0])
        content1.append(data.json()["data"])
        page = page + 500
        sleep(1)
    orgsdata = {}
    for listers in content1:
        for lister in listers:

            orgs = dict(lister)
            orgsnew = {}
            for key, value in orgs.items():
                # print(key)
                # print(value)
                if options.get(orgfields.get(key)) == None:
                    pass
                else:
                    if value == None:
                        pass
                    else:
                        if key == "address_country":
                            pass
                        else:
                            try:
                                orgs[key] = options[orgfields.get(key)][str(value)]
                            except:
                                print(f"{key}-{value}-{orgfields.get(key)}")
                if key == "pipeline_id":
                    orgs[key] = piper[value]
                if key == "stage_id":
                    orgs[key] = stager[value]

                if orgfields.get(key) == None:
                    orgsnew[key] = orgs[key]
                else:
                    orgsnew[orgfields[key]] = orgs[key]

            orgsnew = flatten(orgsnew)

            orgsdata[orgs["id"]] = orgsnew
    # print(pd_orgs.info)
    pd_orgs = pd.DataFrame.from_dict(orgsdata, orient="index")
    print(pd_orgs.info)
    # input('t')
    return pd_orgs


def get_deal_fields(apikey):  # to do get all fields and also all options
    # dealfields
    url = "https://ticketco.pipedrive.com/v1/dealFields/?limit=500&api_token=" + apikey
    content1 = re.get(url)
    # pipelines
    url = "https://ticketco.pipedrive.com/v1/pipelines/?limit=500&api_token=" + apikey
    pipes = re.get(url)
    piper = {}
    for pipe in pipes.json()["data"]:
        # print(pipe)
        piper[pipe["id"]] = pipe["name"]
    print(f"\nPipes: {piper}\n")

    url = "https://ticketco.pipedrive.com/v1/stages/?limit=500&api_token=" + apikey
    stages = re.get(url)
    stager = {}
    for stage in stages.json()["data"]:
        # print(pipe)
        stager[stage["id"]] = stage["name"]
    print(f"\nSTAGES: {stager}\n")

    # print(content1.json())
    # with open("dealfields2.txt", 'w') as f:
    #    f.write(content1.text)

    id_key_name = {}

    all_options = {}
    for field in content1.json()["data"]:
        # key, value in field.items()
        # print(field)

        ## GET OPTIONS
        option = field.get("options")
        if option == None:
            pass
        else:
            all_options_specific = {}
            for dicter in option:
                # Todo Add check if the key exists already
                all_options_specific[str(dicter["id"])] = dicter["label"]
            all_options[field.get("key")] = all_options_specific

        ## GET ID, KEY, and NAME
        id = field["id"]
        keyer = field["key"]
        name = field["name"]
        id_key_name[keyer] = name

        all_options_new = {}
        for key, value in all_options.items():
            all_options_new[id_key_name[key]] = value
            all_options_new[key] = value
    print("\n\n" + str(all_options_new))
    print("\n\n" + str(id_key_name))
    return id_key_name, all_options_new, stager, piper


def get_specific_deal(pdkey, dealid):
    dealidstr = str(dealid)
    url = (
        "https://ticketco.pipedrive.com/v1/deals/"
        + dealidstr
        + "/?limit=500&api_token="
        + apikey
    )
    content = re.get(url)

    # id er identifikatoren for dealen
    # org
    # plattform_id="a91c3689eaacc7d43291417a474523fd6dd4b2b8"
    return content.json()["data"]


def get_all_deals_api(apikey):

    getlist = "Deal - Territory,Deal - Pipeline,Deal - Stage,Organization - Customer segment,Deal - Title,Deal - Next activity date,Deal - Owner,Deal - Type of deal,Deal - Value,Deal - Expected REVENUE,Organization - Name,Deal - Organization,Organization - Belongs to division,Deal - Expected CONTRACT LENGHT,Deal - Expected FIRST TIME of GTV/Revenue,Deal - Expected close date,Deal - Deal value quality,Organization - Visitor adress,Deal - Deal source,Deal - Contact person,Deal - Status,Deal - Won time,Deal - Last stage change,Deal - Currency of Expected REVENUE,Deal - Currency"
    content1 = []
    page = 0
    while True:
        print(f"Page: {page}")
        url = (
            "https://ticketco.pipedrive.com/v1/deals/?limit=500&api_token="
            + apikey
            + "&start="
            + str(page)
        )
        data = re.get(url)
        # print(data.status_code)
        # print(data.json()['data'])
        if (data.json()["data"]) == None:
            print(f"** Breaking @ {page} **")
            break
        # print(data.text)
        # print(data.json()['data'][0])
        content1.append(data.json()["data"])
        page = page + 500
        sleep(1)
    # disabled to deploy as cloud function with read-only storage
    # with open("test_pipedrive_deals_test.txt", "w", encoding="utf-8") as f:
    #    f.write(str(content1))

    # print(content1)
    # input("pause")
    return content1


def country_codes(numeric_country):
    country_list = {}
    alpha_country = country_list[numeric_country]

    return alpha_country


def update_ZA_deals_fields(fields):
    pass


def update_ZA_deals_data(apikey, csvdata):

    workspace = "Zoho CRM Reports"
    report = "Pipedrive All Deals"

    url = (
        "https://reportsapi.zoho.com/api/"
        + "kaare.bottolfsen@ticketco.net/"
        + workspace
        + "/"
        + report
        + "?ZOHO_ACTION=IMPORT"
        +
        # "&ZOHO_DELIMITER=2" +
        "&ZOHO_OUTPUT_FORMAT=JSON&ZOHO_ERROR_FORMAT=JSON"
        + "&ZOHO_API_VERSION=1.0"
        + "&authtoken="
        + apikey
        + "&ZOHO_IMPORT_TYPE=TRUNCATEADD"
        + "&ZOHO_AUTO_IDENTIFY=TRUE"
        + "&ZOHO_ON_IMPORT_ERROR=ABORT"
        + "&ZOHO_CREATE_TABLE=TRUE"
        + "&ZOHO_FILE=deals.csv"
    )
    # print(url)
    file = {"ZOHO_FILE": ("deals.csv", csvdata, "multipart/form-data")}
    con = re.post(url, files=file)
    print(con.text)
    return con.text


if __name__ == "__main__":
    main("","")

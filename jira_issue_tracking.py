#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: jira_issue_tracking.py
Description: script to create jira issue for newly submitted data request 
             and generate Jira changeLogs table
Contributors: Hannah Calkins, Dan Lu
 
"""
import json
import logging
import os

# import pdb
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytz
import requests
import synapseclient
from jira import JIRA
from requests.auth import HTTPBasicAuth
from synapseclient import Table
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseNoCredentialsError,
)

# adapted from challengeutils https://github.com/Sage-Bionetworks/challengeutils/pull/121/files to manage Synapse connection
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class Synapse:
    """Define Synapse class"""

    _synapse_client = None

    @classmethod
    def client(cls, syn_user=None, syn_pass=None, *args, **kwargs):
        """Gets a logged in instance of the synapseclient.
        Args:
            syn_user: Synapse username
            syn_pass: Synpase password
        Returns:
            logged in synapse client
        """
        if not cls._synapse_client:
            LOGGER.debug("Getting a new Synapse client.")
            cls._synapse_client = synapseclient.Synapse(*args, **kwargs)
            try:
                if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
                    secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
                    cls._synapse_client.login(
                        silent=True, authToken=secrets["SYNAPSE_AUTH_TOKEN"]
                    )
                else:
                    cls._synapse_client.login(
                        authToken=os.getenv("SYNAPSE_AUTH_TOKEN"), silent=True
                    )
            except SynapseAuthenticationError:
                cls._synapse_client.login(syn_user, syn_pass, silent=True)

        LOGGER.debug("Already have a Synapse client, returning it.")
        return cls._synapse_client

    @classmethod
    def reset(cls):
        """Change synapse connection"""
        cls._synapse_client = None


def get_folder_name(synapse_id):
    syn = Synapse().client()
    return syn.get(synapse_id, downloadFile=False).name


def create_issue(auth, submission):
    ## ticket creation section below
    ## logic -  from ACT API pull unique value list of resultsId for any request
    ## for each id returned check if jira ticket with id exists.
    ## if ticket does not yet exist, create new jira ticket with below call

    url = "https://sagebionetworks.jira.com/rest/api/3/issue"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    # calculate due date
    duedate = (date.today() + timedelta(days=2)).strftime("%Y-%m-%d")
    summary = f"1kD Access Request: {submission['folder_name']}/ Requester: {submission['submitter']} ({submission['team_name']})"
    team = "-".join(map(str, submission["folder_name"].split("_")[:-1]))
    description = f"Email subject: 1kD Access Request: {submission['folder_name']} / Reply by: <one week from date email is sent> [Tracking: {submission['submission_id']}] \n\nDear<approver name>: \n\nPlease reply to this email by <one week from date email is sent> with your approval decision. If you cannot approve this request, please provide a valid reason for denial to begin a one-week period for further comment and discussion with the requester. \n --- \n\nNote from ACT: <This can be omitted if no special notes from ACT to reviewer are necessary. This is a place to include ACT comment that could help with PI review.> \n\nWe have received a Data Access Request for access to: \n{submission['folder_name']}({submission['synapse_id']})\n\nDate of Request: {submission['submitted_on']}  \n\nLead PI: {submission['project_lead']} \n\nOrganization: {submission['institution']} \n\nIntended Data Use Statement: {submission['IDU']} \n\nData Accessor: {submission['submitter']}(Synapse user_name: {submission['user_name']})"
    payload = json.dumps(
        {
            "fields": {
                "project": {"key": "ONEKD"},
                "summary": summary,
                "duedate": duedate,
                "components": [{"name": "Data Access Request Process"}],
                "customfield_12178": submission["submission_id"],
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"text": description, "type": "text"}],
                        }
                    ],
                },
                "issuetype": {"id": "10144"},
            }
        }
    )

    # custom fields to add: access request ID, access requirement ID, requester name, requester institution, IDU?

    response = requests.request("POST", url, data=payload, headers=headers, auth=auth)


def reformat_datetime(date_time: str):
    """Function to drop milliseconds and tzinfo

    Args:
        date_time: datetime timestamp
    """
    date_time = pd.to_datetime(date_time)
    date_time = date_time.replace(microsecond=0, tzinfo=None)
    return date_time


# def pull_active_issues(auth):
#    url = "https://sagebionetworks.jira.com/rest/api/3/search"
#    headers = {
#       "Accept": "application/json",
#       "Content-Type": "application/json"
#    }
#
#    payload = json.dumps( {
#    "expand": [
#       "names",
#       "changelog"
#    ],
#    "jql": "project = ONEKD AND component = 'Data Access Request Process' AND status not in ('PI Approved','ACT Rejection','PI Rejection','Cancelled','Closed')",
#    "maxResults": 1000,
#    "fields": [
#       "key", #issue number
#       "assignee",
#       "summary",
#       "status",
#       "customfield_12178",
#       "duedate", #for open status
#       "created"
#    ],
#    "startAt": 0
#    } )

#    response = requests.request(
#       "POST",
#       url,
#       data=payload,
#       headers=headers,
#       auth=auth
#    )

#    # convert json t dict
#    results = json.loads(response.text)
#    results = results['issues']
#    logs = pd.DataFrame()
#    for result in results:
#       log = result['changelog']['histories']
#       # if an issue has been processed
#       if log:
#          log = pd.concat([pd.DataFrame({**x['items'][0], **{'created':x['created']}},index=[0]) for x in log]).reset_index(drop=True)
#          # filter out status log
#          if 'status' in log['field'].unique():
#             log = log.loc[log['field'] == 'status', ['fromString', 'toString', 'created']]
#             #reverse order of rows
#             log = log[::-1].reset_index(drop=True)
#             log['created'] = log['created'].apply(reformat_datetime)
#             log['time_in_status'] = log['created'].diff()
#             log['time_in_status'] = log['time_in_status'].shift(-1)
#             # calculate time_in_status for last status
#             log.iloc[-1, log.columns.get_loc('time_in_status')] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log.iloc[-1, log.columns.get_loc('created')]
#             # add other variables
#             log['submission_id'] = result['fields']['customfield_12178']
#             log['key'] = result['key']
#             log['status'] = log['fromString'] + ' - ' + log['toString']
#             log.drop(columns = ['fromString', 'toString'], inplace = True)
#             log['status_order'] = log.index + 1
#             log.rename(columns = {'created' :'createdOn'}, inplace = True)
#             # convert datetime columns to str
#             log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
#             # get column orders to be used later
#             cols = [type + '_' + str(num) for type in ['status', 'createdOn', 'time_in_status'] for num in log.index + 1]
#             cols.sort(key=lambda x: x.split('_')[-1])
#             cols = ['submission_id', 'key'] + cols
#          else:
#             #if anythong other than status changed
#             log = pd.DataFrame({**{'key':result['key'], 'submission_id' : result['fields']['customfield_12178'], 'status': result['fields']['status']['name'], 'createdOn' : result['fields']['created']}},index=[0]).reset_index(drop=True)
#             log['createdOn'] = log['createdOn'].apply(reformat_datetime)
#             log['time_in_status'] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log['createdOn']
#             log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
#             log['status_order'] = 1

#          logs = pd.concat([logs, log],axis = 0, ignore_index=True)
#       else:
#          log = pd.DataFrame({**{'key':result['key'], 'submission_id' : result['fields']['customfield_12178'], 'status': result['fields']['status']['name'], 'createdOn' : result['fields']['created']}},index=[0]).reset_index(drop=True)
#          log['createdOn'] = log['createdOn'].apply(reformat_datetime)
#          log['time_in_status'] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log['createdOn']
#          log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
#          log['status_order'] = 1
#          logs = pd.concat([logs, log],axis = 0, ignore_index=True)
#    return (logs.reset_index(drop = True))


def get_issue_log(issue):
    """Function to pull log information for each issue

    Args:
        issue: a dictionary for the issue
    """
    log = issue["changelog"]["histories"]
    # if an issue has been processed
    if log:
        log = pd.concat(
            [
                pd.DataFrame({**x["items"][0], **{"created": x["created"]}}, index=[0])
                for x in log
            ]
        ).reset_index(drop=True)

        # filter out status log
        if "status" in log["field"].unique():
            log = log.loc[
                log["field"] == "status", ["fromString", "toString", "created"]
            ]
            # reverse order of rows
            log = log[::-1].reset_index(drop=True)
            log["created"] = log["created"].apply(reformat_datetime)
            log["time_in_status"] = log["created"].diff()
            log["time_in_status"] = log["time_in_status"].shift(-1)
            # calculate time_in_status for last status
            log.iloc[-1, log.columns.get_loc("time_in_status")] = (
                datetime.now(pytz.timezone("US/Pacific")).replace(
                    microsecond=0, tzinfo=None
                )
                - log.iloc[-1, log.columns.get_loc("created")]
            )
            # add other variables
            log["submission_id"] = issue["fields"]["customfield_12178"]
            log["key"] = issue["key"]
            log["status"] = log["fromString"] + " - " + log["toString"]
            log.drop(columns=["fromString", "toString"], inplace=True)
            log["status_order"] = log.index + 1
            log.rename(columns={"created": "createdOn"}, inplace=True)
            # convert datetime columns to str
            log[["createdOn", "time_in_status"]] = log[
                ["createdOn", "time_in_status"]
            ].astype(str)
            # get column orders to be used later
            cols = [
                type + "_" + str(num)
                for type in ["status", "createdOn", "time_in_status"]
                for num in log.index + 1
            ]
            cols.sort(key=lambda x: x.split("_")[-1])
            cols = ["submission_id", "key"] + cols

        else:
            # if anythong other than status changed
            log = pd.DataFrame(
                {
                    **{
                        "key": issue["key"],
                        "submission_id": issue["fields"]["customfield_12178"],
                        "status": issue["fields"]["status"]["name"],
                        "createdOn": issue["fields"]["created"],
                    }
                },
                index=[0],
            ).reset_index(drop=True)
            log["createdOn"] = log["createdOn"].apply(reformat_datetime)
            log["time_in_status"] = (
                datetime.now(pytz.timezone("US/Pacific")).replace(
                    microsecond=0, tzinfo=None
                )
                - log["createdOn"]
            )
            log[["createdOn", "time_in_status"]] = log[
                ["createdOn", "time_in_status"]
            ].astype(str)
            log["status_order"] = 1
    else:
        log = pd.DataFrame(
            {
                **{
                    "key": issue["key"],
                    "submission_id": issue["fields"]["customfield_12178"],
                    "status": issue["fields"]["status"]["name"],
                    "createdOn": issue["fields"]["created"],
                }
            },
            index=[0],
        ).reset_index(drop=True)
        log["createdOn"] = log["createdOn"].apply(reformat_datetime)
        log["time_in_status"] = (
            datetime.now(pytz.timezone("US/Pacific")).replace(
                microsecond=0, tzinfo=None
            )
            - log["createdOn"]
        )
        log[["createdOn", "time_in_status"]] = log[
            ["createdOn", "time_in_status"]
        ].astype(str)
        log["status_order"] = 1

    return log


def get_all_issues(auth):
    # Set the query parameters
    # Send the initial request
    url = "https://sagebionetworks.jira.com/rest/api/3/search"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "expand": ["names", "changelog"],
        "jql": "project = ONEKD AND component = 'Data Access Request Process'",
        "maxResults": 100,
        "fields": [
            "key",  # issue number
            "assignee",
            "summary",
            "status",
            "customfield_12178",
            "duedate",  # for open status
            "created",
        ],
        "startAt": 0,
    }
    json_payload = json.dumps(payload)
    response = requests.post(url, data=json_payload, headers=headers, auth=auth)

    # Check that the request was successful
    if response.status_code != 200:
        raise ValueError("Failed to retrieve issues: {}".format(response.content))
    # Get the JSON data from the response
    results = response.json()
    logs = pd.DataFrame()
    # Loop through the issues in the results
    for issue in results["issues"]:
        log = get_issue_log(issue)
        logs = pd.concat([logs, log], axis=0, ignore_index=True)
    # Check if there are more pages of results
    while results["total"] > (payload["startAt"] + payload["maxResults"]):
        # Increment the startAt parameter
        payload["startAt"] += payload["maxResults"]
        # Convert the updated payload to a JSON string
        json_payload = json.dumps(payload)
        # Send the next request with the updated payload
        response = requests.post(url, data=json_payload, headers=headers, auth=auth)
        # Check that the request was successful
        if response.status_code != 200:
            raise ValueError("Failed to retrieve issues: {}".format(response.content))
        # Get the JSON data from the response
        results = response.json()
        # Loop through the issues in the data
        for issue in results["issues"]:
            log = get_issue_log(issue)
            logs = pd.concat([logs, log], axis=0, ignore_index=True)
    return logs.reset_index(drop=True)


def main():
    syn = Synapse().client()
    if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
        secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
        auth = HTTPBasicAuth(secrets["JIRA_EMAIL"], secrets["JIRA_API_TOKEN"])
    else:
        auth = HTTPBasicAuth(
            os.environ.get("JIRA_EMAIL"), os.environ.get("JIRA_API_TOKEN")
        )
    # pull data request info from data request tracking table
    query = "SELECT * from syn51086699"
    requests = syn.tableQuery(query).asDataFrame().reset_index(drop=True)
    # get the submission_id and requestor info
    requests = requests.astype(str)
    requests = requests.loc[requests["controlled_state"] != "CANCELLED",][
        [
            "synapse_id",
            "request_id",
            "submission_id",
            "submitter",
            "user_name",
            "team_name",
            "submitted_on",
            "institution",
            "project_lead",
            "IDU",
        ]
    ]
    # add folder_name
    requests["folder_name"] = requests["synapse_id"].apply(lambda x: get_folder_name(x))
    # logs = pull_all_issues(auth)
    logs = get_all_issues(auth)
    # temp1 = [x for x in requests['submission_id'].unique() if x not in logs['submission_id'].unique()]
    # temp2 = [x for x in logs['submission_id'].unique() if x not in requests['submission_id'].unique()]
    # pdb.set_trace()
    # generate new issue
    for submission_id in requests["submission_id"].unique():
        # pdb.set_trace()
        # exclude some submission_ids because they are either test/cancelled requests or have been tracked in an existing Jira ticket
        if submission_id not in np.append(
            logs["submission_id"].unique(),
            ["5103", "5104", "6163", "5138", "5165", "6357", "5984", "6356"],
        ):
            # pull submission info
            submission = requests.loc[
                requests["submission_id"] == submission_id,
            ].to_dict("records")[0]
            print(f"Creating a new Jira issue for submission_id {submission_id}")
            create_issue(auth, submission)
    # update changeLogs table
    # pdb.set_trace()
    results = syn.tableQuery("select * from syn35358355")
    delete_out = syn.delete(results)
    table_out = syn.store(Table("syn35358355", logs))


if __name__ == "__main__":
    main()

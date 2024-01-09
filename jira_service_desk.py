#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Name: jira_service_desk.py
Description: The script to create jira issue for newly submitted data request in service desk
             and generate Jira changeLogs table for Elite project
Contributors: Dan Lu

"""
import json
import logging
import os
import pdb
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytz
import requests
import synapseclient
from dateutil import parser
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


def create_issue(auth, request):
    ## ticket creation section below
    ## logic -  from ACT API pull unique value list of resultsId for any request
    ## for each id returned check if jira ticket with id exists.
    ## if ticket does not yet exist, create new jira ticket with below call
    url = "https://sagebionetworks.jira.com/rest/servicedeskapi/request"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    # calculate due date
    summary = f"ELITE Access Request: {request['controlled_ar_name']}/ Requester: {request['submitter']}"
    description = f"Email subject: ELITE Access Request: {request['controlled_ar_name']} / Reply by: <4 days from date of email> [Tracking: {request['request_id']}] \n\nDear<approver name>: \n\nPlease reply to this email by <4 days from date of email> with your approval decision. \n --- \n\nNote from ACT: <This can be omitted if no special notes from ACT to reviewer are necessary. This is a place to include ACT comment that could help with PI review.> \n\nWe have received a Data Access Request for access to: \n{request['controlled_ar_name']} ({request['synapse_id']})\n\nDate of Request: {request['submitted_on']}  \n\nProject Lead: {request['project_lead']} \n\nInstitution: {request['institution']} \n\nIntended Data Use Statement: {request['IDU']} \n\nData Requester(s): {request['accessor']} (Synapse user_name: {request['accessor_username']})"
    payload = json.dumps(
        {
            "serviceDeskId": "8",
            "requestTypeId": "163",
            "requestFieldValues": {
                "summary": summary,
                "description": description,
                "customfield_12309": request["request_id"],
            },
        }
    )
    response = requests.request("POST", url, data=payload, headers=headers, auth=auth)
    # print(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))


def reformat_datetime(date_time: str):
    """Function to drop milliseconds and tzinfo

    Args:
        date_time: datetime timestamp
    """
    date_time = pd.to_datetime(date_time)
    date_time = date_time.replace(microsecond=0, tzinfo=None)
    return date_time


def get_issue_log(issue):
    """Function to pull log information for each issue

    Args:
        issue: a dictionary for the issue
    """
    log = issue["changelog"]["histories"]
    if log:
        # if an issue has been processed
        log = pd.json_normalize(
            log, record_path=["items"], meta=["created"]
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
            # calculate time_in_status for the first status
            log.iloc[0, log.columns.get_loc("time_in_status")] = str(
                abs(
                    parser.isoparse(issue["fields"]["created"]).replace(
                        microsecond=0, tzinfo=None
                    )
                    - log.iloc[0, log.columns.get_loc("created")]
                )
            )
            # add other variables
            log["request_id"] = issue["fields"]["customfield_12309"]
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
            cols = ["request_id", "key"] + cols

        else:
            # if anything other than status changed
            log = pd.DataFrame(
                {
                    **{
                        "key": issue["key"],
                        "request_id": issue["fields"]["customfield_12309"],
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
        # if an issue has not been processed
        log = pd.DataFrame(
            {
                **{
                    "key": issue["key"],
                    "request_id": issue["fields"]["customfield_12309"],
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
    url = "https://sagebionetworks.jira.com/rest/api/3/search"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "expand": ["names", "changelog"],
        "jql": "project = 'TESTSD'",
        "maxResults": 100,
        "fields": [
            "key",  # issue number
            "assignee",
            "summary",
            "status",
            "customfield_12309",
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
    if not results["issues"]:
        logs = pd.DataFrame(
            columns=[
                "key",
                "request_id",
                "status",
                "createdOn",
                "time_in_status",
                "status_order",
            ]
        )
    else:
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
                raise ValueError(
                    "Failed to retrieve issues: {}".format(response.content)
                )
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
    query = "SELECT * from syn53240459"
    requests = syn.tableQuery(query).asDataFrame().reset_index(drop=True)
    # get the submission_id and requestor info
    requests = requests.astype(str)
    requests = requests.loc[requests["controlled_state"] != "CANCELLED",][
        [
            "controlled_ar",
            "controlled_ar_name",
            "synapse_id",
            "request_id",
            "submission_id",
            "submitter",
            "accessor",
            "accessor_username",
            "submitted_on",
            "institution",
            "project_lead",
            "IDU",
        ]
    ]
    requests = requests.iloc[0:2,]
    # logs = pull_all_issues(auth)
    logs = get_all_issues(auth)
    # generate new issue
    for request_id in requests["request_id"].unique():
        # exclude some submission_ids because they are either test/cancelled requests or have been tracked in an existing Jira ticket
        if request_id not in logs["request_id"].unique():
            # pull request info
            request = requests.loc[requests["request_id"] == request_id,].to_dict(
                "records"
            )[0]
            print(f"Creating a new Jira issue for request_id {request_id}")
            create_issue(auth, request)
    # pull most recent logs
    logs = get_all_issues(auth)
    # update changeLogs table
    results = syn.tableQuery("select * from syn53240580")
    delete_out = syn.delete(results)
    table_out = syn.store(Table("syn53240580", logs))


if __name__ == "__main__":
    main()

"""
Name: elite_data_request_tracking.py
Description: a script to generate a log table and create Jira tickets for ELITE data requests
Contributors: Dan Lu
"""
import json
import logging
import os
import pdb
import re
import shutil
import subprocess
import tempfile
import typing
from ast import Dict
from datetime import date, datetime, timedelta
from functools import partial
from itertools import chain
from xmlrpc.client import Boolean

import numpy as np
import pandas as pd
import pytz
import requests
import synapseclient
from dateutil import parser
from requests.auth import HTTPBasicAuth
from synapseclient import File, Table
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseNoCredentialsError,
)
from synapseutils import walk

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


def print_green(t: str, st: str = None):
    """
    Function to print out message in green

    :param t (str): a string
    :param st (str, optional): the latter string. Defaults to None. Use this when you want to print out the latter string.
    """
    if st is None:
        print("\033[92m {}\033[00m".format(t))
    else:
        print("{}: \033[92m {}\033[00m".format(t, st))


def from_iso_to_datetime(date_time: str):
    """
    Function to convert ISO 8601 of UTC time to US/Pacific date time and drop milliseconds

    :param date_time (str): datetime string

    :returns: reformated datetime string
    """
    date_time = pd.to_datetime(date_time)
    date_time = date_time.dt.tz_convert("US/Pacific").dt.strftime("%Y-%m-%d %H:%M:%S")
    return pd.to_datetime(date_time)


def get_submission(accessRequirementId: str):
    """
    Function to retrieve a list of submissions to a given controlled access requirement id

    :param accessRequirementId (str): a given controlled access requirement id

    :returns: a list of submissions for a controlled access requirement id
    """
    syn = Synapse().client()
    # get the access requirement lists for the access requirement id
    search_request = {"accessRequirementId": str(accessRequirementId)}
    submissions = syn.restPOST(
        uri=f"/accessRequirement/{str(accessRequirementId)}/submissions",
        body=json.dumps(search_request),
    )["results"]
    return submissions


def get_ar_folder_id(accessRequirementId: str):
    """
    Function to get the synapse id of a folder that has an access requirements
    (used to update synapse_id for data requests submitted from subfolders)

    :param accessRequirementId (str): a given controlled access requirement id

    :returns: a data frame containing a folder id and its access requirement id and access requirement name
    """
    syn = Synapse().client()
    ar = syn.restGET(f"/accessRequirement/{accessRequirementId}")
    ar = pd.DataFrame(
        {
            "accessRequirementId": ar["id"],
            "accessRequirementName": ar["name"],
            "synapse_id": ",".join([e["id"] for e in ar["subjectIds"]]),
        },
        index=[0],
    )
    ar = ar.astype("string")
    return ar


def get_request_id(submission: list) -> set:
    """
    Function to get request ids for a given controlled access requirement id. The requestId is
    the id of the request for a submitter to a folder; this value
    does not change even though a submitter resubmits the data request(multiple submission ids).

    :param submission (list): a list of submissions for a given controlled access requirement id

    :returns: a set of request ids
    """
    request_id = set([i["requestId"] for i in submission])
    return request_id


def group_submissions(submission: list, get_lastest: bool):
    """
    Function to group submissions to a controlled access requirement id by request ids. There are one or more submissions
    each request id

    :param submission (list): a list of submissions for a given controlled access requirement id
    :param get_lastest (boolean): whether to get the latest submission for the request id

    :returns: a list of latest submissions for the given controlled access requirement id if get_lastest is True.
    Otherwise, a list of all submissions for a controlled access requirement id and grouped by request id.
    """
    if get_lastest:
        groups = [
            [i for i in submission if i["requestId"] == a][-1]
            for a in get_request_id(submission)
        ]
    else:
        groups = [
            [i for i in submission if i["requestId"] == a]
            for a in get_request_id(submission)
        ]
    return groups


def get_request_table(
    grouped_submission: list, include_rejectedReason: bool
) -> pd.DataFrame:
    """
    Function to reformat the data request submission list as a data frame

    :param grouped_submission (list): a list of grouped submissions for a given controlled access requirement id
    :param include_rejectedReason (boolean): a list of submissions for a given controlled access requirement id

    :returns: a data frame of the data request submission to a controlled access requirement id
    """
    if include_rejectedReason:
        # used in log table. Use submittedOn to track submission date for each submission
        df = pd.concat(
            [
                pd.DataFrame(
                    {
                        **{"synapse_id": x["subjectId"]},
                        **{"controlled_ar": x["accessRequirementId"]},
                        **{"request_id": x["requestId"]},
                        **{"submission_id": x["id"]},
                        **{"submitter_id": x["submittedBy"]},
                        **{
                            "accessor_id": ",".join(
                                [a["userId"] for a in x["accessorChanges"]]
                            )
                        },
                        **{"institution": x["researchProjectSnapshot"]["institution"]},
                        **{"project_lead": x["researchProjectSnapshot"]["projectLead"]},
                        **{
                            "IDU": x["researchProjectSnapshot"][
                                "intendedDataUseStatement"
                            ]
                        },
                        **{"controlled_state": x["state"]},
                        **{
                            "rejectedReason": x["rejectedReason"]
                            if x["state"] == "REJECTED"
                            else ""
                        },
                        **{"reviewer_id": x["modifiedBy"]},
                        **{"submitted_on": x["submittedOn"]},
                        **{"modified_on": x["modifiedOn"]},
                    },
                    index=[0],
                )
                for x in grouped_submission
            ]
        )
    else:
        df = pd.concat(
            [
                pd.DataFrame(
                    {
                        **{"synapse_id": x["subjectId"]},
                        **{"controlled_ar": x["accessRequirementId"]},
                        **{"request_id": x["requestId"]},
                        **{"submission_id": x["id"]},
                        **{"submitter_id": x["submittedBy"]},
                        **{
                            "accessor_id": ",".join(
                                [a["userId"] for a in x["accessorChanges"]]
                            )
                        },
                        **{"institution": x["researchProjectSnapshot"]["institution"]},
                        **{"project_lead": x["researchProjectSnapshot"]["projectLead"]},
                        **{
                            "IDU": x["researchProjectSnapshot"][
                                "intendedDataUseStatement"
                            ]
                        },
                        **{"controlled_state": x["state"]},
                        **{"reviewer_id": x["modifiedBy"]},
                        **{"created_on": x["researchProjectSnapshot"]["createdOn"]},
                        **{"modified_on": x["modifiedOn"]},
                    },
                    index=[0],
                )
                for x in grouped_submission
            ]
        )
    return df


def calculate_processing_time(row):
    """
    Function to calculate processing_time
    :param row: a dataframe row
    :returns: row with processing_time column added
    """
    if row["controlled_state"] == "SUBMITTED":
        # calculate processing_time for SUBMITTED request (no modifiedOn date, no reviewer)
        row["processing_time"] = (
            datetime.now(pytz.timezone("US/Pacific")).replace(
                microsecond=0, tzinfo=None
            )
            - row["submitted_on"]
        )
        row["modified_on"] = ""
        row["reviewer_id"] = ""
    else:
        row["processing_time"] = row["modified_on"] - row["submitted_on"]
    row[["submitted_on", "modified_on", "processing_time"]] = row[
        ["submitted_on", "modified_on", "processing_time"]
    ].astype(str)
    return row


def data_request_logs(submission: list) -> pd.DataFrame:
    """
    Function to generate logs for each data request

    :param submission (list): a list of submissions for a given controlled access requirement id
    :param ar_table (dataframe): a data frame contains accessRequirementId, accessRequirement name, synapse_id

    :returns: a data frame of all data requests to a controlled access requirement id
    """
    grouped_submissions = group_submissions(submission, get_lastest=False)
    logs = get_request_table(
        grouped_submission=list(chain.from_iterable(grouped_submissions)),
        include_rejectedReason=True,
    )
    logs["state_order"] = logs.groupby(["request_id"]).cumcount() + 1
    # convert time to pst and calculate processing_time
    logs[["submitted_on", "modified_on"]] = logs[["submitted_on", "modified_on"]].apply(
        from_iso_to_datetime
    )
    # add processing_time column
    logs = logs.apply(calculate_processing_time, axis=1)
    # get rid of Non-ASCII characters
    logs.IDU.replace({r"[^\x00-\x7F]+": ""}, regex=True, inplace=True)
    return logs


def update_table(table_name: str, df: pd.DataFrame):
    """
    Function to update table with table name

    :param table_name (str): a table name
    :param df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    tables = {"Data Request changeLogs Table": "syn53038887"}
    results = syn.tableQuery(f"select * from {tables[table_name]}")
    delete_out = syn.delete(results)
    table_out = syn.store(Table(tables[table_name], df))
    print_green(f"Done updating {table_name} table")


def main():
    syn = Synapse().client()
    # get the ar on the folder
    controlled_ars = ["9606120"]
    ar_table = pd.concat(
        [get_ar_folder_id(ar) for ar in controlled_ars], ignore_index=True
    )
    # genetate data request log table
    logs = pd.DataFrame()
    for controlled_ar in controlled_ars:
        submissions = get_submission(controlled_ar)
        if submissions:
            log = data_request_logs(submissions)
            logs = pd.concat([logs, log], axis=0, ignore_index=True)
    # update synapse_id for logs
    logs.drop(columns=["synapse_id"], inplace=True)
    logs = logs.merge(
        ar_table, left_on="controlled_ar", right_on="accessRequirementId", how="left"
    )
    logs.drop(columns=["accessRequirementId"], inplace=True)
    logs.rename(columns={"accessRequirementName": "controlled_ar_name"}, inplace=True)
    # add user profile for logs table
    logs["submitter"] = logs["submitter_id"].apply(
        lambda x: " ".join(
            list(map(syn.getUserProfile(x).get, ["firstName", "lastName"]))
        )
    )
    logs["accessor_id"] = logs["accessor_id"].str.split(",")
    logs["accessor"] = logs["accessor_id"].apply(
        lambda x: ", ".join(
            [
                " ".join(
                    list(map(syn.getUserProfile(e).get, ["firstName", "lastName"]))
                )
                for e in x
            ]
        )
    )
    logs["accessor_username"] = logs["accessor_id"].apply(
        lambda x: ", ".join(
            [" ".join(list(map(syn.getUserProfile(e).get, ["userName"]))) for e in x]
        )
    )
    logs.drop(columns=["submitter_id", "accessor_id"], inplace=True)
    # update tables
    update_table("Data Request changeLogs Table", logs)


if __name__ == "__main__":
    main()

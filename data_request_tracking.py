"""
Name: data_request_tracking.py
Description: a script to generate data_request_tracking table, data request change logs table,
             data structure tree and IDU wiki page for 1kD project
Contributors: Dan Lu
"""

# import modules
import json
import logging
import os

# import pdb
import re
import shutil
import subprocess
import tempfile
import typing
from datetime import datetime
from functools import partial
from itertools import chain

import numpy as np
import pandas as pd
import pytz
import synapseclient
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


def get_user_profile(team_id: str, return_profile: bool = True) -> list:
    """
    Function to get user profile and/or only user id

    :param team_id (str): 1kD team id
    :param return_profile (bool, optional): whether to return user profile. Defaults to True.

    :returns:  list: submitter_id, first_name, last_name, user_name, team_name or only user_id(s)
    """
    syn = Synapse().client()
    members = list(syn.getTeamMembers(syn.getTeam(team_id)))
    if return_profile:
        user_profile = pd.concat(
            [
                pd.DataFrame(
                    {**x["member"], **{"team_id": x["teamId"]}},
                    index=[0],
                )
                for x in members
            ]
        )
        user_profile["team_name"] = syn.getTeam(team_id)["name"]
        user_profile.drop(columns=["isIndividual", "team_id"], inplace=True)
        return user_profile.rename(
            columns={
                "ownerId": "submitter_id",
                "firstName": "first_name",
                "lastName": "last_name",
                "userName": "user_name",
            }
        )
    else:
        user_id = [member["member"]["ownerId"] for member in members]
        return user_id


def get_team_member() -> pd.DataFrame:
    """
    Function to pull team members and re-categorize team members if they are in ACT or admin team

    :returns:  pd.DataFrame: submitter_id, first_name, last_name, user_name, team_name
    """
    # get a list of team members
    team_ids = [
        "3436722",  # 1kD_Connectome
        "3436721",  # 1kD_InfantNaturalStatistics
        "3436720",  # 1kD_BRAINRISE
        "3436718",  # 1kD_KHULA
        "3436509",  # 1kD_MicrobiomeBrainDevelopment
        "3436717",  # 1kD_M4EFaD_LABS
        "3436716",  # 1kD_Assembloids
        "3436713",  # 1kD_DyadicSociometrics_NTU
        "3466183",  # 1kD_DyadicSociometrics_Cambridge
        "3436714",  # 1kD_First1000Daysdatabase
        "3458847",  # 1kD_M4EFaD_BMT team
        "3464137",  # 1kD_Stanford_Yeung team
        "3460645",  # 1kD_M4EFaD_Auckland
    ]
    members = pd.concat([get_user_profile(team_id) for team_id in team_ids])
    # update team name for DCC members
    admin = get_user_profile("3433360")
    act = get_user_profile("464532")
    members.loc[
        members["submitter_id"].isin(admin["submitter_id"].values), "team_name"
    ] = "1kD admins"
    members.loc[
        members["submitter_id"].isin(act["submitter_id"].values), "team_name"
    ] = "ACT"
    # collapse team_name for members that are in multiple teams
    members = (
        members.groupby("submitter_id")["team_name"]
        .agg(lambda x: ",".join(tuple(x.unique())))
        .reset_index()
    )
    members.drop_duplicates(inplace=True)
    return members.reset_index(drop=True)


def get_data_release_folder(folder_id) -> list:
    """
    Function to get data release folder ids in batch

    :param folder_id (str): the parent folder id for data release folders if any

    :returns:  list: data release folder id
    """
    syn = Synapse().client()
    # get study folders
    folders = list(syn.getChildren(folder_id, includeTypes=["folder"]))
    ids = [f["id"] for f in folders]
    # get Data_Release folders
    syn_ids = []
    for id in ids:
        folders = list(syn.getChildren(id, includeTypes=["folder"]))
        for i in folders:
            if "Data" in i["name"]:
                syn_ids.append(i["id"])
    return syn_ids


def create_folder_tree(
    temp_dir, dirpath, clickwrap_ar: str, controlled_ar: str, dirname_mappings={}
):
    """
    Helper function to produces a depth indented listing of directory for a folder

    :param temp_dir (str): dirpath for temp directory
    :param dirpath (str): dirpath form synapseclient.walk, i.e the folder and its id
    :param clickwrap_ar (str): clickWrap accessRequirementId for the given folder
    :param controlled_ar (str): controlled accessRequirementId for the given folder
    :param dirname_mappings: DO NOT CHANGE. empty dictionary to update folder names for temp direcotires

    :returns:  temporary directory is created in the temp_dir folder
    """
    added_ar = ""
    # append AR and synapse_id
    if clickwrap_ar and controlled_ar:
        added_ar = f"{added_ar}_CW-{str(clickwrap_ar)},AR-{str(controlled_ar)}"
    elif clickwrap_ar:
        added_ar = f"{added_ar}_CW-{str(clickwrap_ar)}"

    # update the temporary directory name
    if added_ar:
        new_dirname = f"{dirpath[0]}_{dirpath[1]}_{added_ar}"
    else:
        new_dirname = f"{dirpath[0]}_{dirpath[1]}"
    structure = os.path.join(temp_dir, new_dirname)
    if os.path.dirname(structure) in dirname_mappings.keys():
        structure = structure.replace(
            os.path.dirname(structure), dirname_mappings[os.path.dirname(structure)]
        )
        if os.path.exists(structure):
            shutil.rmtree(structure)
        os.mkdir(structure)
        dirname_mappings[os.path.join(temp_dir, dirpath[0])] = structure
    else:
        if os.path.exists(structure):
            shutil.rmtree(structure)
        os.mkdir(structure)
        dirname_mappings[os.path.join(temp_dir, dirpath[0])] = structure


def get_ar(folder_id: str) -> typing.Generator[str, None, None]:
    """
    Function to retrieve paginated list of all access requirements associated with a folder.

    :param folder_id: a folder id that you'd like to retrieve access requirements for

    :returns: a generator object of access requirements for the given folder
    """
    syn = Synapse().client()
    for result in syn._GET_paginated(uri=f"/entity/{folder_id}/accessRequirement"):
        yield result


def ar_dict(folder_id: str) -> typing.Dict:
    """
    Function to parse out clickWrap and controlAccess access requirement id for a folder

    :param folder_id: a folder id that you'd like to retrieve access requirement id for

    :returns: a dictionary with keys: clickwrap_ar and controlled_ar
    """
    # get accessRequirementId for the target Entity
    all_ar = [ar for ar in get_ar(folder_id)]
    # parse out clickWrap and controlAccess accessRequirementId
    results = {"clickwrap_ar": "", "controlled_ar": ""}
    # some folders have multiple clickwrap_ars
    results["clickwrap_ar"] = ",".join(
        [str(ar["id"]) for ar in all_ar if not "isIDURequired" in ar.keys()]
    )
    results["controlled_ar"] = "".join(
        [str(ar["id"]) for ar in all_ar if "isIDURequired" in ar.keys()]
    )
    return results


def get_folder_tree(out_dir, folder_id) -> typing.Dict:
    """
    Function to get all access requirement ids for data release folder and its subfolders, and to create temporary
    folders in the out_dir to generate tree

    :param out_dir (str): directory to houses the temporary folders
    :param folder_id (str): data release folder synapse_id

    :returns:  a dictionary with keys: clickWrap and controlAccess
    """
    syn = Synapse().client()
    walkedPath = walk(syn, folder_id, ["folder"])
    temp_dir = tempfile.mkdtemp(dir=out_dir)
    dirname_mappings = {}
    results = {"clickwrap_ar": [], "controlled_ar": []}
    for dirpath, dirname, filename in walkedPath:
        # append ARs
        # dirpath is a tuple with first element as folder name and second element as folder id
        clickwrap_ar = ar_dict(dirpath[1])["clickwrap_ar"]
        controlled_ar = ar_dict(dirpath[1])["controlled_ar"]
        # only append non-missing and unique ARs
        if clickwrap_ar:
            for substr in clickwrap_ar.split(","):
                if substr not in results["clickwrap_ar"]:
                    results["clickwrap_ar"].append(substr)
        if controlled_ar and controlled_ar not in results["controlled_ar"]:
            results["controlled_ar"].append(controlled_ar)
        create_folder_tree(
            temp_dir, dirpath, clickwrap_ar, controlled_ar, dirname_mappings
        )
    os.rename(temp_dir, os.path.join(out_dir, folder_id))
    return results


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
    (used to update synapse_id for data requests submitted from assay folders)

    :param accessRequirementId (str): a given controlled access requirement id

    :returns: a data frame containing a folder id and its access requirement id
    """
    syn = Synapse().client()
    ar = syn.restGET(f"/accessRequirement/{accessRequirementId}")
    ar = pd.DataFrame.from_dict(ar)[["id", "subjectIds"]]
    ar["synapse_id"] = ar["subjectIds"].apply(lambda col: col["id"])
    ar.drop(columns="subjectIds", inplace=True)
    ar.rename(columns={"id": "accessRequirementId"}, inplace=True)
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
        return groups
    else:
        groups = {
            a: [i for i in submission if i["requestId"] == a]
            for a in get_request_id(submission)
        }
        return groups


def get_latest_request(submission: list) -> pd.DataFrame:
    """
    Function to get the latest data request submitted from a user to a controlled access requirement id

    :param submission (list): a list of submissions for a given controlled access requirement id

    :returns: a data frame of the latest data requests to a controlled access requirement id
    """
    latest = group_submissions(submission, get_lastest=True)
    # convert list to dataframe
    df = pd.json_normalize(latest, max_level=1)
    # drop modifiedOn in researchProjectSnapshot since it's the same as the createdOn of the snapshot
    df.drop(
        columns=[
            "researchProjectSnapshot.id",
            "researchProjectSnapshot.modifiedOn",
            "researchProjectSnapshot.modifiedBy",
        ],
        inplace=True,
    )
    df = pd.concat(
        [
            df.filter(regex="^researchProjectSnapshot", axis=1),
            df[
                [
                    "subjectId",
                    "state",
                    "requestId",
                    "id",
                    "submittedBy",
                    "modifiedOn",
                    "modifiedBy",
                ]
            ],
        ],
        axis=1,
    )
    # trim and rename dataframe
    df = df.rename(columns=lambda x: re.sub("^researchProjectSnapshot.", "", x))
    df = df[
        [
            "subjectId",
            "accessRequirementId",
            "requestId",
            "id",
            "submittedBy",
            "institution",
            "projectLead",
            "intendedDataUseStatement",
            "state",
            "modifiedBy",
            "createdOn",
            "modifiedOn",
        ]
    ]
    # update subjectId since requestors might submit request from assay folders instead of dataType folders that ar is associated with
    ar_folder = get_ar_folder_id(df["accessRequirementId"].values[0])
    df["subjectId"] = df["accessRequirementId"].map(
        dict(zip(ar_folder["accessRequirementId"], ar_folder["synapse_id"]))
    )
    # rename columns
    df.rename(
        columns={
            "subjectId": "synapse_id",
            "accessRequirementId": "controlled_ar",
            "requestId": "request_id",
            "id": "submission_id",
            "submittedBy": "submitter_id",
            "intendedDataUseStatement": "IDU",
            "projectLead": "project_lead",
            "state": "controlled_state",
            "modifiedBy": "reviewer_id",
            "createdOn": "created_on",
            "modifiedOn": "modified_on",
        },
        inplace=True,
    )
    # convert submittedOn and modifiedOn to datetime data type
    date_cols = ["created_on", "modified_on"]
    df[date_cols] = df[date_cols].apply(from_iso_to_datetime)
    df["total_duration"] = df["modified_on"] - df["created_on"]
    # recalculate total duration for submitted entry
    df.loc[df.controlled_state == "SUBMITTED", "total_duration"] = (
        datetime.now(pytz.timezone("US/Pacific")).replace(microsecond=0, tzinfo=None)
        - df["created_on"]
    )
    # convert date_time columns to string columns
    df[["created_on", "modified_on", "total_duration"]] = df[
        ["created_on", "modified_on", "total_duration"]
    ].astype(str)
    df.loc[df.controlled_state == "SUBMITTED", "modified_on"] = ""
    df.loc[df.controlled_state == "SUBMITTED", "reviewer_id"] = ""
    # get rid of Non-ASCII characters
    df.IDU.replace({r"[^\x00-\x7F]+": ""}, regex=True, inplace=True)
    return df


def data_request_logs(submission: list) -> pd.DataFrame:
    """
    Function to generate logs for each data request

    :param submission (list): a list of submissions for a given controlled access requirement id

    :returns: a data frame of all data requests to a controlled access requirement id
    """
    logs = pd.DataFrame()
    grouped_submissions = group_submissions(submission, get_lastest=False)
    for key, value in grouped_submissions.items():
        log = pd.DataFrame()
        for idx in range(len(value)):
            # loop through submissions
            submission = pd.json_normalize(value[idx], max_level=1)
            if submission["state"].values[0] == "REJECTED":
                # extract reject reason for rejected submission
                submission = pd.concat(
                    [
                        submission.filter(regex="^researchProjectSnapshot", axis=1),
                        submission[
                            [
                                "requestId",
                                "id",
                                "subjectId",
                                "submittedBy",
                                "submittedOn",
                                "modifiedOn",
                                "state",
                                "rejectedReason",
                                "modifiedBy",
                            ]
                        ],
                    ],
                    axis=1,
                )
                # trim and rename dataframe
                submission.drop(
                    columns=[
                        "researchProjectSnapshot.id",
                        "researchProjectSnapshot.modifiedOn",
                        "researchProjectSnapshot.modifiedBy",
                    ],
                    inplace=True,
                )
                submission = submission.rename(
                    columns=lambda x: re.sub("^researchProjectSnapshot.", "", x)
                )
                submission = submission[
                    [
                        "requestId",
                        "id",
                        "subjectId",
                        "accessRequirementId",
                        "submittedBy",
                        "state",
                        "institution",
                        "projectLead",
                        "intendedDataUseStatement",
                        "rejectedReason",
                        "modifiedBy",
                        "submittedOn",
                        "modifiedOn",
                    ]
                ]
            else:
                submission = pd.json_normalize(value[idx], max_level=1)
                submission = pd.concat(
                    [
                        submission.filter(regex="^researchProjectSnapshot", axis=1),
                        submission[
                            [
                                "requestId",
                                "id",
                                "subjectId",
                                "submittedBy",
                                "submittedOn",
                                "modifiedOn",
                                "state",
                                "modifiedBy",
                            ]
                        ],
                    ],
                    axis=1,
                )
                # trim and rename dataframe
                submission.drop(
                    columns=[
                        "researchProjectSnapshot.id",
                        "researchProjectSnapshot.modifiedOn",
                        "researchProjectSnapshot.modifiedBy",
                    ],
                    inplace=True,
                )
                submission = submission.rename(
                    columns=lambda x: re.sub("^researchProjectSnapshot.", "", x)
                )
                submission = submission[
                    [
                        "requestId",
                        "id",
                        "subjectId",
                        "accessRequirementId",
                        "submittedBy",
                        "state",
                        "institution",
                        "projectLead",
                        "intendedDataUseStatement",
                        "modifiedBy",
                        "submittedOn",
                        "modifiedOn",
                    ]
                ]
            # convert time to pst and calculate processing_time
            submission[["submittedOn", "modifiedOn"]] = submission[
                ["submittedOn", "modifiedOn"]
            ].apply(from_iso_to_datetime)
            if value[idx]["state"] == "SUBMITTED":
                # calculate processing_time for SUBMITTED request (no modifiedOn date, no reviewer)
                submission["processing_time"] = (
                    datetime.now(pytz.timezone("US/Pacific")).replace(
                        microsecond=0, tzinfo=None
                    )
                    - submission["submittedOn"]
                )
                submission[
                    ["submittedOn", "modifiedOn", "processing_time"]
                ] = submission[["submittedOn", "modifiedOn", "processing_time"]].astype(
                    str
                )
                submission["modifiedOn"] = ""
                submission["modifiedBy"] = ""
            else:
                # calculate processing_time for other request (no modifiedOn date)
                submission["processing_time"] = (
                    submission["modifiedOn"] - submission["submittedOn"]
                )
                submission[
                    ["submittedOn", "modifiedOn", "processing_time"]
                ] = submission[["submittedOn", "modifiedOn", "processing_time"]].astype(
                    str
                )

            # add state_order
            submission["state_order"] = idx + 1
            # update subjectId since requestors might submit request from assay folders instead of dataType folders that ar is associated with
            ar_folder = get_ar_folder_id(submission["accessRequirementId"].values[0])
            submission["subjectId"] = submission["accessRequirementId"].map(
                dict(zip(ar_folder["accessRequirementId"], ar_folder["synapse_id"]))
            )
            log = pd.concat([log, submission], axis=0, ignore_index=True)
        logs = pd.concat([logs, log], axis=0, ignore_index=True)
    logs.rename(
        columns={
            "requestId": "request_id",
            "id": "submission_id",
            "subjectId": "synapse_id",
            "accessRequirementId": "controlled_ar",
            "submittedBy": "submitter_id",
            "intendedDataUseStatement": "IDU",
            "modifiedBy": "reviewer_id",
            "submittedOn": "submitted_on",
            "modifiedOn": "modified_on",
            "state": "controlled_state",
            "projectLead": "project_lead",
        },
        inplace=True,
    )
    # get rid of Non-ASCII characters
    logs.IDU.replace({r"[^\x00-\x7F]+": ""}, regex=True, inplace=True)
    return logs


def get_clickwrap_request(accessRequirementId: str):
    """
    Function to pull data request submitted via clickWrap

    :param accessRequirementId (str): a clickWrap access requirement id

    :returns: a data frame of all data requests to a clickWrap access requirement id, including submitter_id, synapse_id, clickwrap_ar
    """
    syn = Synapse().client()
    # retrieving a page of AccessorGroup
    accessRequirementId = str(accessRequirementId)
    search_request = {"accessRequirementId": accessRequirementId}
    ag = pd.DataFrame(
        syn.restPOST(uri="/accessApproval/group", body=json.dumps(search_request))[
            "results"
        ]
    )
    # generate clickwrap_request table only if AccessApprovalInfo available
    if not ag.empty:
        # retrieve subjectIds for each accessRequirementId
        cw = get_ar_folder_id(accessRequirementId)
        # add submitter_id
        ag = ag[["accessRequirementId", "submitterId"]].astype("string")
        cw = cw.merge(ag, how="left", on="accessRequirementId")
        cw.rename(
            columns={
                "submitterId": "submitter_id",
                "accessRequirementId": "clickwrap_ar",
            },
            inplace=True,
        )
        cw["clickwrap_state"] = "Accepted"
        return cw


def update_table(table_name: str, df: pd.DataFrame):
    """
    Function to update table with table name

    :param table_name (str): a table name
    :param df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    tables = {
        "Data Request Tracking Table": "syn51086692",
        "Data Request changeLogs Table": "syn51086699",
    }
    results = syn.tableQuery(f"select * from {tables[table_name]}")
    delete_out = syn.delete(results)
    table_out = syn.store(Table(tables[table_name], df))
    print_green(f"Done updating {table_name} table")


def update_folder_tree(out_dir):
    """
    Function to update folder tree file

    :param out_dir (str): the directory saves the temp folders created in get_folder_tree()
    """
    syn = Synapse().client()
    with open("data_folder_structure.txt", "w") as file:
        subprocess.run(["tree", "-d", out_dir], stdout=file)
        table_out = syn.store(
            File(
                "data_folder_structure.txt",
                description="1kD data folder structure and access requirement setting",
                parent="syn35023796",
            )
        )
        os.remove("data_folder_structure.txt")


def generate_idu_wiki(df: pd.DataFrame):
    """
    Function to generate 1kD Data Use Statements Wiki

    :param table_name (str): a table name from which we pull approved data requests
    :param df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    # filter out APPROVED data requests
    df = df.loc[df["controlled_state"] == "APPROVED",]

    # append folder names
    df["folder_name"] = df.apply(
        lambda x: syn.get(x["synapse_id"], downloadFile=False)["name"], axis=1
    )
    df["team_folder"] = df["folder_name"].str.split("_").str[:-1].str.join("_")

    # sort by team_folder and folder_name
    df.sort_values(by=["folder_name", "team_folder"], inplace=True)
    df = df.reset_index()
    # grab the wiki you want to update
    wiki = syn.getWiki(owner="syn26133760", subpageId="621404")

    # build the wiki md
    wiki.markdown = ""
    for team in df.team_folder.unique():
        temp_df = df.loc[df["team_folder"] == team,]
        wiki.markdown += f"### {team} Access Requests "
        for x in temp_df.index:
            wiki.markdown += (
                "\n"
                + "\n **Request ID:** "
                + str(temp_df["submission_id"][x])
                + "\n **Requested Data:** "
                + "["
                + temp_df["folder_name"][x]
                + "]("
                + temp_df["synapse_id"][x]
                + ")"
                + "\n **Status**: "
                + temp_df["controlled_state"][x]
                + "\n **Decision Date: **"
                + temp_df["modified_on"][x]
                + "\n **Project Lead: **"
                + temp_df["project_lead"][x]
                + "\n **IDU Statement: **"
                + temp_df["IDU"][x]
                + "\n"
            )

    # update the wiki
    wiki = syn.store(wiki)


def main():
    Synapse().client()
    ## crawl through folder structure to get accessRequirementId
    folder_ids = get_data_release_folder("syn26294912")
    # create a temporary directory under the current working directory
    out_dir = tempfile.mkdtemp(dir=os.getcwd())
    get_folder_tree_temp = partial(get_folder_tree, out_dir)
    results = {"clickwrap_ar": [], "controlled_ar": []}
    for folder_id in folder_ids:
        result = get_folder_tree_temp(folder_id)
        results["clickwrap_ar"].append(result["clickwrap_ar"])
        results["controlled_ar"].append(result["controlled_ar"])
    # update folder tree and remove temporary directory once done
    update_folder_tree(out_dir)
    shutil.rmtree(out_dir)
    # drop duplicates
    clickwrap_ars = set(chain.from_iterable(results["clickwrap_ar"]))
    controlled_ars = set(chain.from_iterable(results["controlled_ar"]))
    ## genetate data request log table
    logs = pd.DataFrame()
    latest_requests = pd.DataFrame()
    for controlled_ar in controlled_ars:
        submissions = get_submission(controlled_ar)
        if submissions:
            log = data_request_logs(submissions)
            logs = pd.concat([logs, log], axis=0, ignore_index=True)
            latest_request = get_latest_request(submissions)
            latest_requests = pd.concat(
                [latest_requests, latest_request], axis=0, ignore_index=True
            )
    clickwrap_requests = pd.DataFrame()
    for clickwrap_ar in clickwrap_ars:
        clickwrap_request = get_clickwrap_request(clickwrap_ar)
        clickwrap_requests = pd.concat(
            [clickwrap_requests, clickwrap_request], axis=0, ignore_index=True
        )
    # regenerate accessRequirementId column since some folders can have multiple clickwrap_ar
    clickwrap_requests.groupby(["synapse_id", "submitter_id"])["clickwrap_ar"].apply(
        lambda x: ",".join(x)
    ).reset_index()
    # merge the click-wrap and latest controlled data requests
    ar_merged = pd.merge(
        latest_requests,
        clickwrap_requests,
        how="left",
        on=["synapse_id", "submitter_id"],
    )
    # add user profile
    members = get_team_member()
    ar_merged = ar_merged.merge(members, how="left", on="submitter_id")
    ar_merged.drop(columns=["submitter_id"], inplace=True)

    logs = logs.merge(members, how="left", on="submitter_id")
    logs[["first_name", "last_name"]] = logs[["first_name", "last_name"]].astype(str)
    logs["submitter"] = logs[["first_name", "last_name"]].agg(" ".join, axis=1)
    logs.drop(columns=["submitter_id", "first_name", "last_name"], inplace=True)

    # update tables and wiki
    update_table("Data Request Tracking Table", ar_merged)
    update_table("Data Request changeLogs Table", logs)
    generate_idu_wiki(ar_merged)


if __name__ == "__main__":
    main()

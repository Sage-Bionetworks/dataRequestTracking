"""
Name: data_request_tracking.py
Description: a script to generate data_request_tracking table, data request change logs table,
             data structure tree and IDU wiki page for 1kD project
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

# import modules
from ast import Dict
from datetime import datetime
from functools import partial
from itertools import chain
from xmlrpc.client import Boolean

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

    :returns:  list: submitter_id, submitter, user_name, team_name or only user_id(s)
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
        user_profile[["firstName", "lastName"]] = user_profile[
            ["firstName", "lastName"]
        ].astype(str)
        user_profile["submitter"] = user_profile[["firstName", "lastName"]].agg(
            " ".join, axis=1
        )
        user_profile.drop(
            columns=["isIndividual", "team_id", "firstName", "lastName"], inplace=True
        )
        user_profile.rename(
            columns={"ownerId": "submitter_id", "userName": "user_name"}, inplace=True
        )
        return user_profile
    else:
        user_id = [member["member"]["ownerId"] for member in members]
        return user_id


def get_team_member() -> pd.DataFrame:
    """
    Function to pull team members and re-categorize team members if they are in ACT or admin team

    :returns:  pd.DataFrame: submitter_id, submitter, user_name, team_name
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
        members.groupby(["submitter_id", "submitter", "user_name"], dropna=False)[
            "team_name"
        ]
        .apply(lambda x: ",".join(x.unique()))
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


def get_latest_request(submission: list) -> pd.DataFrame:
    """
    Function to get the latest data request submitted from a user to a controlled access requirement id

    :param submission (list): a list of submissions for a given controlled access requirement id

    :returns: a data frame of the latest data requests to a controlled access requirement id
    """
    latest = group_submissions(submission, get_lastest=True)
    # convert list to dataframe
    df = get_request_table(grouped_submission=latest, include_rejectedReason=False)
    # convert submittedOn and modifiedOn to datetime data type
    df[["created_on", "modified_on"]] = df[["created_on", "modified_on"]].apply(
        from_iso_to_datetime
    )
    df["total_duration"] = df["modified_on"] - df["created_on"]
    # recalculate total duration for submitted entry
    df["total_duration"] = np.where(
        df["controlled_state"] == "SUBMITTED",
        (
            datetime.now(pytz.timezone("US/Pacific")).replace(
                microsecond=0, tzinfo=None
            )
            - df["created_on"]
        ),
        df["total_duration"],
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


def get_clickwrap_request(accessRequirementId: str) -> pd.DataFrame:
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
        # add submitter_id
        cw = ag[["accessRequirementId", "submitterId"]].astype("string")
        # cw = cw.merge(ag, how="left", on="accessRequirementId")
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
        "Controlled Access Requirement Table": "syn52539497",
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
            ),
            forceVersion=False,
        )
        os.remove("data_folder_structure.txt")


def get_folder_name(synapse_id: str):
    """
    Function to get folder name

    :param synapse_id (str): a Synapse ID for the folder
    """
    syn = Synapse().client()
    folder_name = syn.get(synapse_id, downloadFile=False)["name"]
    return f"[{folder_name}]({synapse_id})"


def get_team_name(folder_name: str):
    """
    Function to get team name based on folder name

    :param folder_name (str): a folder_name list
    """
    team_name = re.split(r"\[|\]", folder_name)[1].split("_")[:-1]
    team_name = "_".join(team_name)
    return team_name


def generate_idu_wiki(df: pd.DataFrame):
    """
    Function to generate 1kD Data Use Statements Wiki

    :param table_name (str): a table name from which we pull approved data requests
    :param df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    # filter out APPROVED data requests and AR of Leap test project
    df = df.loc[df["controlled_state"] == "APPROVED",]
    df = df.loc[df["controlled_ar"] != "9606042",]

    # append folder names
    df["synapse_id"] = df["synapse_id"].str.split(",")
    df["folder_name"] = df["synapse_id"].apply(lambda x: list(map(get_folder_name, x)))
    df["team_folder"] = df["folder_name"].apply(lambda x: set(map(get_team_name, x)))
    # conver list to strings
    df["folder_name"] = df["folder_name"].apply(lambda x: ", ".join(x))
    df["folder_name"] = df.apply(
        lambda x: f"{x['controlled_ar_name']} ({x['folder_name']})", axis=1
    )
    df["team_folder"] = df["team_folder"].apply(lambda x: "".join(x))

    # sort by team_folder and folder_name
    df.sort_values(by=["team_folder"], inplace=True)
    df = df.reset_index()
    # grab the wiki you want to update
    wiki = syn.getWiki(owner="syn26133760", subpageId="621404")

    # build the wiki md
    wiki.markdown = ""
    for team in df.team_folder.unique():
        temp_df = df.loc[df["team_folder"] == team,]
        wiki.markdown += f"### {str(team)} Access Requests "
        for x in temp_df.index:
            wiki.markdown += (
                "\n"
                + "\n **Request ID:** "
                + str(temp_df["submission_id"][x])
                + "\n **Requested Data:** "
                + temp_df["folder_name"][x]
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
    # crawl through folder structure to get accessRequirementId
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
    # update_folder_tree(out_dir)
    shutil.rmtree(out_dir)
    # drop duplicates
    clickwrap_ars = set(chain.from_iterable(results["clickwrap_ar"]))
    controlled_ars = set(chain.from_iterable(results["controlled_ar"]))
    # generate controlled ar table
    ar_table = pd.concat(
        [get_ar_folder_id(ar) for ar in clickwrap_ars.union(controlled_ars)],
        ignore_index=True,
    )
    # genetate data request log table
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
    # get synapse_id
    clickwrap_requests = pd.merge(
        clickwrap_requests,
        ar_table,
        left_on="clickwrap_ar",
        right_on="accessRequirementId",
        how="left",
    )
    clickwrap_requests.drop(
        columns=["accessRequirementId", "accessRequirementName"], inplace=True
    )
    clickwrap_requests["synapse_id"] = clickwrap_requests["synapse_id"].str.split(",")
    clickwrap_requests = clickwrap_requests.explode("synapse_id").reset_index(drop=True)
    clickwrap_requests = (
        clickwrap_requests.groupby(["synapse_id", "submitter_id", "clickwrap_state"])[
            "clickwrap_ar"
        ]
        .apply(lambda x: ",".join(x))
        .reset_index()
    )
    # convert latest_requets to long to merge with the clickWrap
    latest_requests.drop(columns=["synapse_id"], inplace=True)
    latest_requests = latest_requests.merge(
        ar_table, left_on="controlled_ar", right_on="accessRequirementId", how="left"
    )
    latest_requests.drop(columns=["accessRequirementId"], inplace=True)
    latest_requests["synapse_id"] = latest_requests["synapse_id"].str.split(",")
    latest_requests = latest_requests.explode("synapse_id").reset_index(drop=True)
    latest_requests.rename(
        columns={"accessRequirementName": "controlled_ar_name"}, inplace=True
    )
    # merge the click-wrap and latest controlled data request
    ar_merged = pd.merge(
        latest_requests,
        clickwrap_requests,
        how="left",
        on=["synapse_id", "submitter_id"],
    )
    # collapse synapse_id
    ar_merged_index = ar_merged.columns.to_list()
    ar_merged_index.remove("synapse_id")
    ar_merged = (
        ar_merged.groupby(ar_merged_index, dropna=False)["synapse_id"]
        .apply(lambda x: ",".join(x))
        .reset_index()
    )

    # update synapse_id for logs
    logs.drop(columns=["synapse_id"], inplace=True)
    logs = logs.merge(
        ar_table, left_on="controlled_ar", right_on="accessRequirementId", how="left"
    )
    logs.drop(columns=["accessRequirementId"], inplace=True)
    logs.rename(columns={"accessRequirementName": "controlled_ar_name"}, inplace=True)

    # load team member table
    members = get_team_member()
    members = members.set_index("submitter_id").to_dict("index")

    # add user profile for ar_merged table
    ar_merged["submitter"] = ar_merged["submitter_id"].apply(
        lambda x: members.get(x, {}).get("submitter")
    )
    ar_merged["team_name"] = ar_merged["submitter_id"].apply(
        lambda x: members.get(x, {}).get("team_name")
    )
    ar_merged["accessor_id"] = ar_merged["accessor_id"].str.split(",")
    ar_merged["accessor"] = ar_merged["accessor_id"].apply(
        lambda x: ", ".join([str(members.get(e, {}).get("submitter")) for e in x])
    )
    ar_merged.drop(columns=["submitter_id", "accessor_id"], inplace=True)

    # add user profile for logs table
    logs["submitter"] = logs["submitter_id"].apply(
        lambda x: members.get(x, {}).get("submitter")
    )
    logs["team_name"] = logs["submitter_id"].apply(
        lambda x: members.get(x, {}).get("team_name")
    )
    logs["accessor_id"] = logs["accessor_id"].str.split(",")
    logs["accessor"] = logs["accessor_id"].apply(
        lambda x: ", ".join([str(members.get(e, {}).get("submitter")) for e in x])
    )
    logs["accessor_username"] = logs["accessor_id"].apply(
        lambda x: ", ".join([str(members.get(e, {}).get("user_name")) for e in x])
    )
    logs.drop(columns=["submitter_id", "accessor_id"], inplace=True)
    # pdb.set_trace()

    # update tables and wiki
    update_table("Data Request Tracking Table", ar_merged)
    update_table("Data Request changeLogs Table", logs)
    update_table("Controlled Access Requirement Table", ar_table)
    generate_idu_wiki(ar_merged)


if __name__ == "__main__":
    main()

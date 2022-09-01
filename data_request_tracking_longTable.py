'''
Name: data_request_tracking_longTable.py
Description: a script to generate data_request_tracking table, data request change logs table, 
             data structure tree and team member table for 1kD project
Contributors: Dan Lu
'''
# import modules
import json
import logging
import multiprocessing
import os
import pdb
import re
import shutil
import subprocess
import tempfile
from datetime import datetime
from functools import partial
from itertools import chain

import numpy as np
import pandas as pd
import pytz
import synapseclient
from synapseclient import File, Table
from synapseclient.core.exceptions import (SynapseAuthenticationError,
                                           SynapseNoCredentialsError)
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
                    cls._synapse_client.login(silent=True, authToken=secrets["SYNAPSE_AUTH_TOKEN"])
                else:
                    cls._synapse_client.login(authToken = os.getenv("SYNAPSE_AUTH_TOKEN"),silent=True)
            except SynapseAuthenticationError:
                cls._synapse_client.login(syn_user, syn_pass, silent=True)

        LOGGER.debug("Already have a Synapse client, returning it.")
        return cls._synapse_client

    @classmethod
    def reset(cls):
        """Change synapse connection"""
        cls._synapse_client = None


def prGreen(t, st=None):  # print for success
    """Function to print out message in green

    Args:
        t (str): a string
        st (str, optional): the latter string. Defaults to None. 
        Use this when you want to print out the latter string.
    """
    if st is None:
        print("\033[92m {}\033[00m".format(t))
    else:
        print("{}: \033[92m {}\033[00m".format(t, st))


def get_userProfile(teamID: str, return_profile: bool = True) -> list:
    """Function to get user profile and/id
    Args:
        teamID (str): 1kD team ID
        return_profile (bool, optional): whether to return user profile. Defaults to True.

    Returns:
        list: userProfile (submitterID, firstName, lastName, userName, teamId, isAdmin, team) or only userIDs
    """
    syn = Synapse().client()
    members = list(syn.getTeamMembers(syn.getTeam(teamID)))
    if return_profile:
        user_profile = pd.concat(
            [
                pd.DataFrame(
                    {**x["member"], **{"teamId": x["teamId"], "isAdmin": x["isAdmin"]}},
                    index=[0],
                )
                for x in members
            ]
        )
        user_profile.drop(columns="isIndividual", inplace = True)
        user_profile["team"] = syn.getTeam(teamID)["name"]
        return user_profile.rename(columns={"ownerId": "submitterID"})
    else:
        userIDs = [member["member"]["ownerId"] for member in members]
        return userIDs


def get_team_members() -> pd.DataFrame:
    """Function to pull all team members

    Returns:
        pd.DataFrame: contains submitterID, firstName, lastName, userName, teamName
    """
    syn = Synapse().client()
    # get a list of team members
    teamIDs = [
        "3436722",
        "3436721",
        "3436720",
        "3436719",
        "3436718",
        "3436509",
        "3436717",
        "3436716",
        "3436713",
        "3436714",
    ]
    team_members = pd.concat([get_userProfile(teamID) for teamID in teamIDs])
    # update team name for DCC members
    team_members["submitterID"].value_counts()
    team_members.loc[
        team_members["submitterID"].isin(["273995", "3445322", "3434599"]), "team"
    ] = "1kD admins"
    team_members.loc[
        team_members["submitterID"].isin(["273995", "3445322", "3434599"]), "teamId"
    ] = "3433360"
    team_members.drop_duplicates(inplace=True)
    team_members.drop(columns="isAdmin", inplace=True)

    # add governance team members
    gov_team = ["273977", "3429359"]
    gov_team_members = pd.concat(
        [pd.DataFrame(syn.getUserProfile(member), index=[0]) for member in gov_team]
    )[["ownerId", "firstName", "lastName", "userName"]]
    gov_team_members["team"] = "1kD admins"
    gov_team_members["teamId"] = "3433360"
    gov_team_members.rename(columns={"ownerId": "submitterID"}, inplace=True)
    members = pd.concat([team_members, gov_team_members])
    members.drop(columns="teamId", inplace=True)
    members.rename(columns={"team": "teamName"}, inplace=True)
    return members.reset_index(drop=True)


def get_data_folderIDs() -> list:
    """Function to get Data Release foldersIDs

    Returns:
        list: Data Release folder SynapseIDs
    """
    syn = Synapse().client()
    # get study folders
    folders = list(syn.getChildren("syn26294912", includeTypes=["folder"]))
    IDs = [f["id"] for f in folders]
    # get Data_Release folders
    synIDs = []
    for id in IDs:
        folders = list(syn.getChildren(id, includeTypes=["folder"]))
        for i in folders:
            if "Data" in i["name"]:
                synIDs.append(i["id"])
    return synIDs

def get_folder_tree(
    temp_dir, dirpath, clickWrap_AR: str, controlled_AR: str, dirname_mappings={}
):
    """Function to produces a depth indented listing of directory for a Synapse folder
       and attach AR info
    Args:
        temp_dir : path for temp directory
        dirpath: dirpath form synapseclient.walk
        clickWrap_AR (str): clickWrap accessRequirement ID for the given folder
        controlled_AR (str): controlled accessRequirement ID for the given folder
    Returns: temporary directory is created in the temp_dir folder
    """
    added_ar = ""
    if clickWrap_AR:
        added_ar = f"{added_ar}_CW-{str(clickWrap_AR[0])}"
    if controlled_AR and "CW" in added_ar:
        added_ar = f"{added_ar},AR-{str(controlled_AR[0])}"
    elif controlled_AR:
        added_ar = f"{added_ar}_AR-{str(controlled_AR[0])}"
    if added_ar:
        new_dirname = f"{dirpath[0]}{added_ar}"
    else:
        new_dirname = dirpath[0]
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


def get_AR_folders(out_dir, folderID) -> pd.DataFrame:
    """function to get datafolders with AR information and to create temporary
      folders in the out_dir to generate tree
    Args:
        out_dir (str): directory to houses the temporary folders
        folderID (str): data release folder SynapseID

    Returns: pd.DataFrame: a dataframe contains data folder name, Synapse ID, clickWrap_AR and controlled_AR
    """
    syn = Synapse().client()
    folders = pd.DataFrame()
    walkedPath = walk(syn, folderID, ["folder"])
    temp_dir = tempfile.mkdtemp(dir=out_dir)
    dirname_mappings = {}
    for dirpath, dirname, filename in walkedPath:
        folder = pd.DataFrame([dirpath], columns=["folder_name", "folder_ID"])
        # append ARs
        clickWrap_AR, controlled_AR = get_accessRequirementIds(folder.folder_ID[0])
        if clickWrap_AR:
            folder["clickWrap_AR"] = str(clickWrap_AR[0])
        if controlled_AR:
            folder["controlled_AR"] = str(controlled_AR[0])
        get_folder_tree(
                temp_dir, dirpath, clickWrap_AR, controlled_AR, dirname_mappings
            )
        folders = pd.concat([folders, folder], ignore_index=True)
    os.rename(temp_dir, os.path.join(out_dir, folderID))
    return folders


def get_accessRequirementIds(folderID: str) -> list:
    """Function to get accessRequirementId for a given folder

    Args:
        folderID (str): folder SynapseID

    Returns:
        list: clickWrap_AR and controlled_AR
    """
    syn = Synapse().client()
    # TODO: explore why this API called twice for each entity
    all_ar = syn.restGET(uri=f"/entity/{folderID}/accessRequirement")["results"]
    if all_ar:
        clickWrap_AR = [ar["id"] for ar in all_ar if not "isIDURequired" in ar.keys()]
        controlled_AR = [ar["id"] for ar in all_ar if "isIDURequired" in ar.keys()]
    else:
        clickWrap_AR = ""
        controlled_AR = ""
    return clickWrap_AR, controlled_AR


def from_iso_to_datetime(date_time: str):
    """Function to convert ISO 8601 of UTC time to US/Pacific date time and drop milliseconds

    Args:
        date_time (str): datetime string
    """
    date_time = pd.to_datetime(date_time)
    date_time = date_time.dt.tz_convert("US/Pacific").dt.strftime("%Y-%m-%d %H:%M:%S")
    return pd.to_datetime(date_time)


def get_requestIds(ar: list):
    """Function to get unique requestIds for a given access requirement ID
    Args:
        ar (list): a list of submissions for a given access requirement ID
    """
    requestIds = set([i["requestId"] for i in ar])
    return requestIds


def group_submissions(ar: list, requestIds: list, get_current: bool):
    """Function to group submissions to a AR by requestId
       requestId is the ID of the Request which is used to create this submission; this value
       does not change even though a submitter resubmits the data request
    Args:
        ar(list): a list of submissions for a given access requirement ID
        requestIds (list):  A list of requestId for an accessRequirementID
        get_current (boolean): whether to get the current state of the submission
    """
    if get_current:
        groups = [[i for i in ar if i["requestId"] == a][-1] for a in requestIds]
        return groups
    else:
        groups = {a: [i for i in ar if i["requestId"] == a] for a in requestIds}
        return groups


def get_controlled_data_request(accessRequirementId: str):
    """Function to pull data requestor's information and its IDUs
    for approved data access to controlled data
    Args: accessRequirementId - string of the accessRequiement ID
    """
    syn = Synapse().client()
    # get the access requirement lists for the requirementID
    search_request = {"accessRequirementId": str(accessRequirementId)}
    ar = syn.restPOST(
        uri=f"/accessRequirement/{str(accessRequirementId)}/submissions",
        body=json.dumps(search_request),
    )["results"]
    if ar:
        requestIds = get_requestIds(ar)
        grouped_submissions = group_submissions(ar, requestIds, get_current=True)
        # convert list to dataframe
        df = pd.json_normalize(grouped_submissions, max_level=1)
        # drop modified on in researchProjectSnapshot
        df.drop(columns=["researchProjectSnapshot.modifiedOn"], inplace=True)
        df = pd.concat(
            [
                df.filter(regex="^researchProjectSnapshot", axis=1),
                df[["subjectId", "state", "requestId", "modifiedOn"]],
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
                "createdBy",
                "institution",
                "projectLead",
                "intendedDataUseStatement",
                "state",
                "createdOn",
                "modifiedOn"
            ]
        ]
        df.rename(
            columns={
                "subjectId": "SynapseID",
                "accessRequirementId": "controlled_AR",
                "createdBy": "submitterID",
                "intendedDataUseStatement": "IDU",
            },
            inplace=True,
        )
        # convert submittedOn and modifiedOn to datetime data type
        date_cols = ["createdOn", "modifiedOn"]
        df[date_cols] = df[date_cols].apply(from_iso_to_datetime)
        df["total_duration"] = df["modifiedOn"] - df["createdOn"]
        # recalculate total duration for submitted entry
        df.loc[df.state == "SUBMITTED", "total_duration"] = (
            datetime.now(pytz.timezone("US/Pacific")).replace(
                microsecond=0, tzinfo=None
            )
            - df["createdOn"]
        )
        df[["createdOn", "modifiedOn", "total_duration"]] = df[
            ["createdOn", "modifiedOn", "total_duration"]
        ].astype(str)
        df.loc[df.state == "SUBMITTED", "modifiedOn"] = ""
        return df


def get_clickWrap_request(accessRequirementId: str, AR_folder: pd.DataFrame):
    """Function to pull data request submitted via clickWrap
    Args: accessRequirementId (str) - accessRequiement ID
         AR_folder - dataframe contains folder_name, folder_ID, clickWrap_AR, and controlled_AR"
    """
    syn = Synapse().client()
    # Retrieve the AccessorGroup for a accessRequirementId
    accessRequirementId = str(accessRequirementId)
    search_request = {"accessRequirementId": accessRequirementId}
    df_ag = pd.DataFrame(
        syn.restPOST(uri="/accessApproval/group", body=json.dumps(search_request))[
            "results"
        ]
    )[["accessRequirementId", "submitterId"]]
    # get and trim accessRequirement description
    df_ar = AR_folder.loc[AR_folder["clickWrap_AR"] == accessRequirementId,["clickWrap_AR", "folder_ID", "controlled_AR"],].drop_duplicates()
    df_ar.rename(
        columns={
            "folder_ID": "SynapseID",
            "clickWrap_AR": "accessRequirementId",
        },
        inplace=True,
    )
    # merge with AccessorGroup
    df = df_ar.merge(df_ag, how="cross")[
        [
            "SynapseID",
            "accessRequirementId_x",
            "submitterId",
            "controlled_AR",
        ]
    ]
    df.rename(
        columns={
            "accessRequirementId_x": "clickWrap_AR",
            "submitterId": "submitterID"
        },
        inplace=True,
    )
    df["state"] = "Accepted"
    return df


def data_request_processing_logs(accessRequirementId: str) -> pd.DataFrame:
    """Function to trimmed processing logs for each data request

    Args:
        accessRequirementId (str): accessRequirement ID

    Returns:
        pd.DataFrame: a dataframe contains all data request states (requestID)
    """
    syn = Synapse().client()
    accessRequirementId = str(accessRequirementId)
    search_request = {"accessRequirementId": accessRequirementId}
    ar = syn.restPOST(
        uri=f"/accessRequirement/{accessRequirementId}/submissions",
        body=json.dumps(search_request),
    )["results"]
    logs = pd.DataFrame()
    if ar:
        requestIds = get_requestIds(ar)
        grouped_submissions = group_submissions(ar, requestIds, get_current=False)
        for key, value in grouped_submissions.items():
            log = pd.DataFrame()
            for idx in range(len(value)):
                event = pd.json_normalize(value[idx], max_level=1)
                if event["state"].values[0] == "REJECTED":
                    # extract reject reason for rejected event
                    event = pd.concat(
                        [
                            event.filter(regex="^researchProjectSnapshot", axis=1),
                            event[
                                ["submittedOn", "modifiedOn", "state", "rejectedReason"]
                            ],
                        ],
                        axis=1,
                    )
                    # trim and rename dataframe
                    event.drop(
                        columns=[
                            "researchProjectSnapshot.modifiedOn",
                        ],
                        inplace=True,
                    )
                    event = event.rename(
                        columns=lambda x: re.sub("^researchProjectSnapshot.", "", x)
                    )
                    event = event[
                        [
                            "state",
                            "intendedDataUseStatement",
                            "rejectedReason",
                            "submittedOn",
                            "modifiedOn",
                        ]
                    ]
                else:
                    event = pd.json_normalize(value[idx], max_level=1)
                    event = pd.concat(
                        [
                            event.filter(regex="^researchProjectSnapshot", axis=1),
                            event[["submittedOn", "modifiedOn", "state"]],
                        ],
                        axis=1,
                    )
                    # trim and rename dataframe
                    event.drop(
                        columns=[
                            "researchProjectSnapshot.modifiedOn",
                        ],
                        inplace=True,
                    )
                    event = event.rename(
                        columns=lambda x: re.sub("^researchProjectSnapshot.", "", x)
                    )
                    event = event[
                        [
                            "state",
                            "intendedDataUseStatement",
                            "submittedOn",
                            "modifiedOn",
                        ]
                    ]
                # convert time to pst and calculate time_in_status
                event[["submittedOn", "modifiedOn"]] = event[
                        ["submittedOn", "modifiedOn"]
                    ].apply(from_iso_to_datetime)
                if value[idx]["state"] == "SUBMITTED":
                    # calculate time_in_status for SUBMITTED request (no modifiedOn date)
                    event["time_in_status"] = (
                        datetime.now(pytz.timezone("US/Pacific")).replace(
                            microsecond=0, tzinfo=None
                        )
                        - event["submittedOn"]
                    )
                    event[["submittedOn", "modifiedOn", "time_in_status"]] = event[
                        ["submittedOn", "modifiedOn", "time_in_status"]
                    ].astype(str)
                    event["modifiedOn"] = ""
                else:
                    event["time_in_status"] = (
                        event["modifiedOn"] - event["submittedOn"]
                    )
                    event[["submittedOn", "modifiedOn", "time_in_status"]] = event[
                        ["submittedOn", "modifiedOn", "time_in_status"]
                    ].astype(str)
                event['state_order'] = idx + 1
                log = pd.concat([log, event], axis=0, ignore_index=True)
            log["requestId"] = key
            logs = pd.concat([logs, log], axis=0, ignore_index=True)
        logs.rename(columns = {"intendedDataUseStatement":"IDU"}, inplace= True)
        return logs


def update_table(tableName: str, df: pd.DataFrame):
    """Function to update table with table name

    Args:
        tableName (str): table name
        df (pd.DataFrame): the data frame to be saved
    """
    syn = Synapse().client()
    tables = {
        "Data Request Tracking Table": "syn33240664",
        "Data Request changeLogs Table": "syn35382746",
        '1kD Team Members': "syn35048407",
    }
    results = syn.tableQuery(f"select * from {tables[tableName]}")
    delete_out = syn.delete(results)
    table_out = syn.store(Table(tables[tableName], df))
    prGreen(f"Done updating {tableName} table")


def update_folder_tree(out_dir):
    syn = Synapse().client()
    file = open("data_folder_structure.txt", "w")
    subprocess.run(["tree", "-d", out_dir], stdout=file)
    table_out = syn.store(
        File(
            "data_folder_structure.txt",
            description="1kD data folder structure and AR setting",
            parent="syn35023796",
        )
    )
    os.remove("data_folder_structure.txt")

def main():
    Synapse().client()
    ## crawl through folder structure to get accessRequirementId
    folderIDs = get_data_folderIDs()
    #create a temporary directory under the current working directory
    out_dir = tempfile.mkdtemp(dir = os. getcwd())
    #get_AR_folders('/home/ec2-user/1KD-DCC/helpers/TEMP', 'syn29763372')
    get_AR_folders_temp = partial(get_AR_folders, out_dir)
    results = map(get_AR_folders_temp, folderIDs)
    AR_folders = pd.concat(results)
    # update folder tree and remove temporary directory once done
    update_folder_tree(out_dir)
    shutil.rmtree(out_dir)
    # get accessRequirementIds and drop duplicates
    clickWrap_ARs = np.unique(AR_folders['clickWrap_AR'].dropna()) 
    controlled_ARs = np.unique(AR_folders['controlled_AR'].dropna())
    ## genetate data request log table
    logs = pd.DataFrame()
    for controlled_AR in controlled_ARs:
        log = data_request_processing_logs(controlled_AR)
        logs = pd.concat([logs, log], axis=0, ignore_index=True)
    ## pull data request info
    controlled_requests = pd.DataFrame()
    for controlled_AR in controlled_ARs:
        controlled_request = get_controlled_data_request(controlled_AR)
        controlled_requests = pd.concat([controlled_requests, controlled_request])

    clickWrap_requests = pd.DataFrame()
    for clickWrap_AR in clickWrap_ARs:
        clickWrap_request = get_clickWrap_request(clickWrap_AR, AR_folders)
        clickWrap_requests = pd.concat([clickWrap_requests, clickWrap_request])
    
    # add clickWrap_AR to controlled_requests
    controlled_requests["clickWrap_AR"] = controlled_requests["SynapseID"].map(
        dict(zip(AR_folders["folder_ID"], AR_folders["clickWrap_AR"]))
    )
    # merge the click-wrap and controlled data requests, not use SynapseID here since one pair of clickWrap_AR and controlled_AR appplied to multiple entities
    controlled_requests = controlled_requests.set_index(
        ["clickWrap_AR", "controlled_AR", "submitterID"]
    )
    clickWrap_requests = clickWrap_requests.set_index(
        ["clickWrap_AR", "controlled_AR", "submitterID"]
    )
    # merge controlled and clickwrap
    df_merged = pd.merge(
        controlled_requests,
        clickWrap_requests,
        how="outer",
        left_index=True,
        right_index=True,
    )
    df_merged.reset_index(inplace=True)
    df_merged.drop(columns=["SynapseID_x"], inplace=True)
    df_merged.rename(
        columns={
            "state_x": "controlled_state",
            "state_y": "clickWrap_state",
            "SynapseID_y": "SynapseID",
        },
        inplace=True,
    )
    # add user profile
    members = get_team_members()
    df_merged = df_merged.merge(members, how="left", on="submitterID")
    df_merged = df_merged[
        [
            "SynapseID",
            "clickWrap_AR",
            "clickWrap_state",
            "controlled_AR",
            "controlled_state",
            "requestId",
            #"submitterID",
            "firstName",
            "lastName",
            "userName",
            "teamName",
            "institution",
            "projectLead",
            "IDU",
            "createdOn",
            "modifiedOn",
            "total_duration",
        ]
    ]
    # trim df_merged to only include requests being processed or be in process
    df_merged = df_merged[(df_merged[["clickWrap_AR","controlled_AR", "requestId"]].notnull().all(1)) | ((~df_merged["clickWrap_AR"].isnull()) & (df_merged["controlled_AR"].isnull()))].reset_index(drop=True)
    #update tables
    #update_table("Data Request Tracking Table", df_merged)
    update_table("Data Request changeLogs Table", logs)
    #update_table("1kD Team Members", members)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Name: jira_issue_tracking_longTable.py
Description: script to create jira issue for newly submitted data request 
             and generate issue tracking report
Contributors: Hannah Calkins, Dan Lu
 
'''
import json
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytz
import requests
import synapseclient
from requests.auth import HTTPBasicAuth
from synapseclient import Table
from synapseclient.core.exceptions import (SynapseAuthenticationError,
                                           SynapseNoCredentialsError)

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

def create_issue(auth, summary : str, duedate : str, requestId : str, description : str):
   ## ticket creation section below 
   ## logic -  from ACT API pull unique value list of resultsId for any request 
   ## for each id returned check if jira ticket with id exists. 
   ## if ticket does not yet exist, create new jira ticket with below call

   url = "https://sagebionetworks.jira.com/rest/api/3/issue"
   headers = {
      "Accept": "application/json",
      "Content-Type": "application/json"
   }

   payload=json.dumps({
   "fields": {
      "project":
      { 
         "key": "ONEKD"
      },
      "summary": summary,
      "duedate": duedate, 
      "components" : [{"name": "Data Access Request Process"}],
      "customfield_12178":requestId,
      "description": {
         "type": "doc",
         "version": 1,
         "content": [
         {
            "type": "paragraph",
            "content": [
               {
               "text": description,
               "type": "text"
               }
            ]
         }
         ]
      },
      "issuetype": {
         "id": "10144" 
      }
   }
   })

   #custom fields to add: access request ID, access requirement ID, requester name, requester institution, IDU?    
    
   response = requests.request(
      "POST",
      url,
      data=payload,
      headers=headers,
      auth=auth
   )

def reformat_datetime(date_time : str):
  """Function to drop milliseconds and tzinfo

  Args:
      date_time: datetime timestamp
  """
  date_time = pd.to_datetime(date_time)
  date_time = date_time.replace(microsecond = 0, tzinfo = None)
  return (date_time)  

def pull_issues(auth):
   url = "https://sagebionetworks.jira.com/rest/api/3/search"
   headers = {
      "Accept": "application/json",
      "Content-Type": "application/json"
   }

   payload = json.dumps( {
   "expand": [
      "names",
      "changelog"
   ],
   "jql": "project = ONEKD AND component = 'Data Access Request Process' AND status not in ('Approved', 'Final Denial', 'Closed')",
   "maxResults": 250,
   "fields": [
      "key", #issue number
      "assignee",
      "summary",
      "status",
      "customfield_12178",
      "duedate", #for open status
      "created"
   ],
   "startAt": 0
   } )

   response = requests.request(
      "POST",
      url,
      data=payload,
      headers=headers,
      auth=auth
   )

   # convert json t dict
   results = json.loads(response.text)
   results = results['issues']
   logs = pd.DataFrame()
   for result in results:
      log = result['changelog']['histories']
      # if an issue has been processed
      if log: 
         log = pd.concat([pd.DataFrame({**x['items'][0], **{'created':x['created']}},index=[0]) for x in log]).reset_index(drop=True)
         # filter out status log 
         if 'status' in log['field'].unique(): 
            log = log.loc[log['field'] == 'status', ['fromString', 'toString', 'created']]
            #reverse order of rows
            log = log[::-1].reset_index(drop=True)    
            log['created'] = log['created'].apply(reformat_datetime)
            log['time_in_status'] = log['created'].diff()
            log['time_in_status'] = log['time_in_status'].shift(-1)
            # calculate time_in_status for last status
            log.iloc[-1, log.columns.get_loc('time_in_status')] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log.iloc[-1, log.columns.get_loc('created')]
            # add other variables
            log['requestId'] = result['fields']['customfield_12178']
            log['key'] = result['key']
            log['status'] = log['fromString'] + ' - ' + log['toString']
            log.drop(columns = ['fromString', 'toString'], inplace = True)
            log['status_order'] = log.index + 1
            log.rename(columns = {'created' :'createdOn'}, inplace = True)
            # convert datetime columns to str
            log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
            # get column orders to be used later
            cols = [type + '_' + str(num) for type in ['status', 'createdOn', 'time_in_status'] for num in log.index + 1]
            cols.sort(key=lambda x: x.split('_')[-1])
            cols = ['requestId', 'key'] + cols
         else:
            #if anythong other than status changed
            log = pd.DataFrame({**{'key':result['key'], 'requestId' : result['fields']['customfield_12178'], 'status': result['fields']['status']['name'], 'createdOn' : result['fields']['created']}},index=[0]).reset_index(drop=True)
            log['createdOn'] = log['createdOn'].apply(reformat_datetime)
            log['time_in_status'] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log['createdOn']
            log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
            log['status_order'] = 1
            
         logs = pd.concat([logs, log],axis = 0, ignore_index=True)
      else: 
         log = pd.DataFrame({**{'key':result['key'], 'requestId' : result['fields']['customfield_12178'], 'status': result['fields']['status']['name'], 'createdOn' : result['fields']['created']}},index=[0]).reset_index(drop=True)
         log['createdOn'] = log['createdOn'].apply(reformat_datetime)
         log['time_in_status'] = datetime.now(pytz.timezone('US/Pacific')).replace(microsecond = 0, tzinfo = None) - log['createdOn']
         log[['createdOn', 'time_in_status']] = log[['createdOn', 'time_in_status']].astype(str)
         log['status_order'] = 1
         logs = pd.concat([logs, log],axis = 0, ignore_index=True)
   return (logs.reset_index(drop = True))

def main():
   syn = Synapse().client()
   if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
      secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
      auth = HTTPBasicAuth(secrets["JIRA_EMAIL"], secrets["JIRA_API_TOKEN"])
   else:
      auth = HTTPBasicAuth(os.environ.get("JIRA_EMAIL"), os.environ.get("JIRA_API_TOKEN"))
   # pull data request info from data request tracking table
   query = "SELECT * from syn33240664"
   request_tracking = syn.tableQuery(query).asDataFrame().reset_index(drop = True)
   # get the requestId and requestor info
   request_tracking=request_tracking[request_tracking.controlled_state.notnull()]
   request_tracking= request_tracking.astype(str)
   request_tracking = request_tracking.loc[request_tracking['controlled_state'] != "APPROVED",][['SynapseID', 'controlled_AR','requestId', 'firstName', 'lastName', 'teamName']]
   request_tracking = request_tracking.reset_index(drop = True)
   # reformat the table 
   request_tracking = request_tracking.groupby(['requestId', 'controlled_AR','firstName', 'lastName', 'teamName']).apply(lambda x: ', '.join(list(x['SynapseID'].unique()))).reset_index()
   request_tracking.rename(columns = {0:'SynapseID'}, inplace = True)
   logs = pull_issues(auth)
   # generate new issue
   summary = 'New data request to 1kD'
   duedate = (date.today() + timedelta(days=2)).strftime('%Y-%m-%d')
   if len(logs) != 0:
      for requestId in request_tracking.requestId.unique():
         if requestId not in logs.requestId.unique():
            description = f"{str(request_tracking.loc[request_tracking['requestId'] == requestId, 'firstName'].values[0])} {request_tracking.loc[request_tracking['requestId'] == requestId, 'lastName'].values[0]} from {request_tracking.loc[request_tracking['requestId'] == requestId, 'teamName'].values[0]} team requested access to {request_tracking.loc[request_tracking['requestId'] == requestId, 'SynapseID'].values[0]}. (Controlled_ID: {request_tracking.loc[request_tracking['requestId'] == requestId, 'controlled_AR'].values[0]})"
            create_issue(auth, summary, duedate, requestId, description)
   else: 
      for requestId in request_tracking.requestId.unique():
         description = f"{str(request_tracking.loc[request_tracking['requestId'] == requestId, 'firstName'].values[0])} {request_tracking.loc[request_tracking['requestId'] == requestId, 'lastName'].values[0]} from {request_tracking.loc[request_tracking['requestId'] == requestId, 'teamName'].values[0]} team requested access to {request_tracking.loc[request_tracking['requestId'] == requestId, 'SynapseID'].values[0]}. (Controlled_ID: {request_tracking.loc[request_tracking['requestId'] == requestId, 'controlled_AR'].values[0]})"
         create_issue(auth, summary, duedate, requestId, description)
   # update changeLogs table
   results = syn.tableQuery("select * from syn35358355")
   delete_out = syn.delete(results)
   table_out = syn.store(Table("syn35358355", logs))

if __name__ == "__main__":
    main()

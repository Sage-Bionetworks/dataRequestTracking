'''
Name: create_tables.py
Description: script to generate data_request_tracking table, data request change logs table，Jira_changeLogs table, and team member table schema
Contributors: Dan Lu
'''
import json
import re
from datetime import datetime
from itertools import chain

import numpy as np
import pandas as pd
import pytz
import synapseclient
from synapseclient import Column, Row, RowSet, Schema, Table, as_table_columns
from synapseutils import walk

syn = synapseclient.Synapse()
syn.login(silent=True)

# construct schema for the data_request_tracking table 
cols = [Column(name='synapse_id', columnType='ENTITYID'),
Column(name='clickwrap_ar', columnType='STRING'),
Column(name='clickwrap_state', columnType='STRING'),
Column(name='controlled_ar', columnType='STRING'),
Column(name='controlled_state', columnType='STRING'),
Column(name='request_id', columnType='STRING'),
Column(name='submission_id', columnType='STRING'),
#Column(name='submitterID', columnType='STRING'),
Column(name='first_name', columnType='STRING'),
Column(name='last_name', columnType='STRING'),
Column(name='user_name', columnType='STRING'),
Column(name='team_name', columnType='STRING'),
Column(name='institution', columnType='STRING'),
Column(name='project_lead', columnType='STRING'),
Column(name= 'IDU', columnType='STRING',maximumSize = 1000),
Column(name= 'created_on', columnType='STRING'),
Column(name= 'modified_on', columnType='STRING'),
Column(name= 'reviewer_id', columnType='STRING'),
Column(name= 'total_duration', columnType='STRING')
]

schema = syn.store(synapseclient.table.Schema(name='Data Request Tracking Table', columns=cols, parent="syn26243167"))
ar_merged = pd.read_csv("")
table = syn.store(Table(schema, ar_merged))

# construct schema for the data_request_changeLogs table 
cols = [Column(name='requestId', columnType='STRING'),
Column(name='state_1', columnType='STRING'),
Column(name='IDU_1', columnType='STRING',maximumSize = 1000),
Column(name='rejectedReason_1', columnType='LARGETEXT'),
Column(name='submittedOn_1', columnType='STRING'),
Column(name='modifiedOn_1', columnType='STRING'),
Column(name='time_in_status__1', columnType='STRING'),
Column(name='state_2', columnType='STRING'),
Column(name= 'IDU_2', columnType='STRING',maximumSize = 1000),
Column(name= 'submittedOn_2', columnType='STRING'),
Column(name= 'modifiedOn_2', columnType='STRING'),
Column(name= 'time_in_status__2', columnType='STRING')
]

schema = syn.store(synapseclient.table.Schema(name='Data Request changeLogs Table', columns=cols, parent="syn26243167"))
table = syn.store(Table(schema, logs))
# long version
cols = [Column(name='requestId', columnType='STRING'),
Column(name='state', columnType='STRING'),
Column(name='state_order', columnType='INTEGER'),
Column(name='IDU', columnType='STRING',maximumSize = 1000),
Column(name='rejectedReason', columnType='LARGETEXT'),
Column(name='submittedOn', columnType='STRING'),
Column(name='modifiedOn', columnType='STRING'),
Column(name='time_in_status', columnType='STRING'),
]

schema = syn.store(synapseclient.table.Schema(name='Data Request changeLogs (longVersion)', columns=cols, parent="syn26133760"))
table = syn.store(Table(schema, logs))

# AR_settings
#cols = [Column(name='dataType_folder_name', columnType='STRING'),
#Column(name='dataType_folder_ID', columnType='STRING'),
#Column(name='dataType_clickWrap_AR', columnType='STRING'),
#Column(name='dataType_controlled_AR', columnType='STRING'),
#Column(name='assay_folder_name', columnType='STRING'),
#Column(name='assay_folder_ID', columnType='STRING')
#]
#schema = syn.store(synapseclient.table.Schema(name='Folders and AR setting', columns=cols, parent="syn26133760"))
#AR_folder = AR_folder[['dataType_folder_name', 'dataType_folder_ID', 'dataType_clickWrap_AR', 'dataType_controlled_AR', 'assay_folder_name', 'assay_folder_ID']]
#table = syn.store(Table(schema, AR_folder))

# jira tracking 
cols = [Column(name='requestId', columnType='STRING'),
Column(name='key', columnType='STRING'),
Column(name='status_1', columnType='STRING'),
Column(name='createdOn_1', columnType='STRING'),
Column(name='time_in_status_1', columnType='STRING'),
Column(name='status_2', columnType='STRING'),
Column(name='createdOn_2', columnType='STRING'),
Column(name='time_in_status_2', columnType='STRING'),
Column(name='status_3', columnType='STRING'),
Column(name='createdOn_3', columnType='STRING'),
Column(name='time_in_status_3', columnType='STRING'),
Column(name='status_4', columnType='STRING'),
Column(name='createdOn_4', columnType='STRING'),
Column(name='time_in_status_4', columnType='STRING'),
Column(name='status_5', columnType='STRING'),
Column(name='createdOn_5', columnType='STRING'),
Column(name='time_in_status_5', columnType='STRING')
]
schema = syn.store(synapseclient.table.Schema(name='Jira_changeLogs', columns=cols, parent="syn26133760"))
table = syn.store(Table(schema, logs.drop(columns =['index'])))
# long version 
cols = [Column(name='requestId', columnType='STRING'),
Column(name='key', columnType='STRING'),
Column(name='status', columnType='STRING'),
Column(name='status_order', columnType='INTEGER'),
Column(name='createdOn', columnType='STRING'),
Column(name='time_in_status', columnType='STRING'),
]

schema = syn.store(synapseclient.table.Schema(name='Jira_changeLogs_longVersion', columns=cols, parent="syn26133760"))
table = syn.store(Table(schema, logs.drop(columns =['index'])))

# team member table
cols = [Column(name='submitterID', columnType='STRING'),Column(name='firstName', columnType='STRING'),Column(name='lastName', columnType='STRING'),Column(name='userName', columnType='STRING'),Column(name='teamName', columnType='STRING')]
schema = syn.store(synapseclient.table.Schema(name='1kD Team Members', columns=cols, parent="syn26133760"))
table = syn.store(Table(schema, members))

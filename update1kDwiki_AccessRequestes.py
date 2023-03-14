#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Name: update1kDwiki_AccessRequestes.py
Description: script to generate 1kD Data Use Statements Wiki
Contributors: Hannah Calkins, Dan Lu
 
'''

import synapseclient
import synapseutils
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
import pandas as pd



syn=synapseclient.login() 

#this is the table that holds ARs for a particular team.  Change this table reference depending on which team's ARs you are trying to display.  Currently, only tables for KHULA and USP exist
results = syn.tableQuery("select * from syn51086692" )

#load the results from the query into a dataframe
df=results.asDataFrame()

# filter out APPROVED data requests
df = df.loc[df['controlled_state'] == 'APPROVED', ]

# append folder names
df['folder_name'] = df.apply(lambda x: syn.get(x['synapse_id'], downloadFile = False)['name'], axis=1)
df['team_folder'] = df['folder_name'].str.split('_').str[:-1].str.join('_')

# deal with Ã in the paragraph
df.IDU.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)

# sort by team_folder and folder_name
df.sort_values(by=['folder_name', 'team_folder'], inplace = True)
df = df.reset_index()
#grab the wiki you want to update
wiki = syn.getWiki(owner= "syn26133760",subpageId='621404')

#build the wiki md 
wiki.markdown = ""
for team in df.team_folder.unique():
    temp_df = df.loc[df['team_folder'] == team,]
    wiki.markdown += f"### {team} Access Requests "
    for x in temp_df.index:
        wiki.markdown += "\n" + "\n **Request ID:** " + str(temp_df['submission_id'][x]) + "\n **Requested Data:** " +  "["+ temp_df['folder_name'][x] +"]("  + temp_df['synapse_id'][x] +")" +"\n **Status**: " + temp_df['controlled_state'][x] + "\n **Decision Date: **" + temp_df['modified_on'][x] + "\n **Project Lead: **" + temp_df['project_lead'][x] + "\n **IDU Statement: **"  + temp_df['IDU'][x] + '\n' 


#update the wiki
wiki = syn.store(wiki)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 09:48:29 2022

@author: hcalkins
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 12:33:14 2022

@author: hcalkins
"""


import synapseclient
import synapseutils
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns
import pandas as pd



syn=synapseclient.login() #add credentials

#this is the table that holds ARs for a particular team.  Change this table reference depending on which team's ARs you are trying to display.  Currently, only tables for KHULA and USP exist
results = syn.tableQuery("select * from syn37859852" )

#load the results from the query into a dataframe
df=results.asDataFrame()

#grab the wiki you want to update
entity = syn.get('syn45559235')
wiki = syn.getWiki(entity)

#build the wiki md 
wiki.markdown="# Access Requests "


for x in range(0,len(df.index)) :
    
    e=syn.get(df['SynapseID'][x])
    wiki.markdown += "\n### Request ID" + df['requestId'][x] + "\n **Requested Data:** " + "["+ e['name'] +"]("  + df['SynapseID'][x] +")" +"\n **Status**: " + df['requestStatus'][x] + "\n **Decision Date: **" + df['modifiedOn'][x] + "\n **Project Lead: **" + df['projectLead'][x] + "\n **IDU Statement: **"  + df['IDU'][x] + '\n' 
    
#update the wiki
wiki = syn.store(wiki)



    
    
    







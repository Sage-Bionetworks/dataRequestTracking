# Introduction

This repository houses scripts and docker image to track data requests submitted to 1kD and Elite_LLFS project, their status and their associated Jira tickets. 

# Data Request Tracking and Reporting Workflow
This diagram shows how data requests are tracked and reported in Synapse and Jira Software Dashboard/Jira Service Desk.
![Data Request Tracking and Reporting (1)](https://github.com/Sage-Bionetworks/dataRequestTracking/assets/90745557/08466ecc-b633-459b-8aa1-09e82f4bfed1)

The `data_request_tracking.py` is used to pull data request history using Synapse Rest API and generate Data Request Tracking Table(1kD Only), Data Request changeLogs Table, and Data Folder Structure.txt. The Data Request Tracking Table is a summary table of the most recent status of a data request, its basic information and total duration. The Data Request changeLogs Table illustrates the detailed change log of each data request, including states, status and basic information for each submission, and its processing time. The data_folder_structure.txt is a file holding the foler tree of the project and the corresponding access requirement setting. 
The `jira_issue_tracking.py` (1kD) and `jira_service_desk.py` (Elite) are used to create Jira ticket/request in Jira Software Dashboard/Jira Service Desk and to update Jira changeLogs Table in Synapse. The Jira changeLogs Table indicates how the Jira ticket/request is processed and the processing time for each status. 

# Setting Up
The pipeline is run under ACT team member's account on AWS Batch(Scheduled Jobs) since only ACT has permission to pull data request from Synapse backend. 
A. The following credentials need to be created and provided in `Batch Secrets`. The format for `Batch Secrets` is 
```"SYNAPSE_AUTH_TOKEN":"Your PAT","JIRA_EMAIL":"Your email in Jira","JIRA_API_TOKEN":"Your Jira API token"```
  1. SYNAPSE_AUTH_TOKEN. The SYNAPSE_AUTH_TOKEN is the Personal Access Token(PAT) that can be used to log in to the Synapse command line, Python or R clients. It can be created [using this instruction](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens). To enable the pipeline to be run successfully, the token needs to have can View, Download and Modify permissions.
  2. JIRA_EMAIL and JIRA_API_TOKEN. The JIRA_EMAIL is the email a user uses in Jira Software Dashboard/Jira Service Desk and the JIRA_API_TOKEN is the token that can be used to authenticate a script to interact with an Atlassian cloud product. The JIRA_API_TOKEN can be created following [this instruction](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/).
  3. Docker image. A docker image needs to be provided in AWS Batch Image. Currently, the sagebionetworks/1kd-data-request-tracking image is used.
  4. AWS Batch Image Schedule needs to be set to indicate how often the pipeline needs to be run.
  5. AWS Batch Command example: ```sh update_tables.sh```
  6. Required tags needs to be provided for cost tracking.
  7. Jira Automation is set up to allow automatic notification sent to the Approver once new ticket is generated. 

**Docker**

There is a Docker image that is automatically build: sagebionetworks/1kd-data-request-tracking. See the available tags [here](https://hub.docker.com/r/sagebionetworks/1kd-data-request-tracking). It is always recommended to use the latest tag since it will be updated whenever changes are made to this GitHub repository. 

# NOTE: Build without caching to ensure latest version of git repo
#       docker build --no-cache -t datarequesttracking .
FROM python:latest

RUN pip3 install synapseclient[pandas,pysftp] 

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN https://github.com/Sage-Bionetworks/dataRequestTracking.git

WORKDIR dataRequestTracking

CMD ["/bin/bash"]


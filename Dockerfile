# NOTE: Build without caching to ensure latest version of git repo
#       docker build --no-cache -t datarequesttracking .
FROM python:3.10-slim-bullseye

RUN apt update \
&&apt-get install -y tree

RUN pip3 install \
    datetime \
    numpy \
    pytz \
    requests \
    synapseclient \
    pandas \
    pysftp

WORKDIR dataRequestTracking
COPY . .


# NOTE: Build without caching to ensure latest version of git repo
#       docker build --no-cache -t datarequesttracking .
FROM python:latest

RUN pip3 install \
	datetime \
	numpy \
	pytz \
	requests \
 	synapseclient \
 	pandas \
	pysftp

RUN git clone https://github.com/Sage-Bionetworks/dataRequestTracking.git

WORKDIR dataRequestTracking

CMD ["/bin/bash"]
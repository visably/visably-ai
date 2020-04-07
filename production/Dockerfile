#################################################################
#  Dockerfile to build production inference code
# Visably, LLC retains all rights to this software
# FHS, Jan 21, 2020
#################################################################
FROM visably/base_nlp 
RUN mkdir -p /inference/model
RUN mkdir /inference/prm
RUN mkdir /inference/source
RUN mkdir /inference/data
RUN mkdir /inference/run


COPY model/ /inference/model
COPY prm/ /inference/prm
COPY source /inference/source
COPY data/ /inference/data
COPY run/ /inference/run


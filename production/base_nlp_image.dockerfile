#################################################################
#  Dockerfile to build base image inference code
# Visably, LLC retains all rights to this software
# FHS, Jan 21, 2020
#################################################################
FROM ubuntu:18.04
RUN apt update -y
RUN apt install -y  python3-pip python3.6
RUN pip3 install --upgrade pip

RUN mkdir /init

COPY requirements.txt /init

RUN pip3 install -r /init/requirements.txt

RUN python3 -m nltk.downloader stopwords
RUN python3 -m spacy download en

ENV LANG C.UTF-8 

#!/bin/bash

docker build -t visably/base_nlp -f base_nlp_image.dockerfile .
docker build -t visably/nlp_classifier .
## Visably Production Classification Model

# Quickstart

1. Download model files from google drive. Model files will consist of random forest model binary and 3 vocabulary csv files
2. Unzip model files to this directory (`visably-ai/production`)
2. Build docker images
```
./build_docker.sh
```
3. Start docker container
```
./start_container
```
4. Change directory to inference script and run inference
```
cd inference/run && ./inference.sh
```
5. Analyze results (default location is `../data/classified_urls.csv`)

# Change Data Files
To run inference on new data beyond the sample data, simply change the values in `wordfeature.prm` to point to your directory of html files (`Direcotry for input URLs`) and change the value for the csv containing URL names (`Filename for word wordfeature URL list`).

# Add New Model
A model is defined by vocubulary files and a model binary. These are bundled in a `model.zip` compressed file. To add a new model, simply download the new model.zip and unzip the contents to the `visably-ai/production` directory.

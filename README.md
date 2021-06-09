# visably-ai
AI Research and Development branch containing all code to generate machine learning models, research notebooks, and pre-production testing code for text extraction and classification

# Quickstart
```
pip3 install -r requirements.txt
cd run
./scrape.run
```

# Overview
Configuration files can be found in `prm` folder. Change these parameters to execute program on different data or with different parameters. Execute programs in `run` folder. Output will be displayed in `out`.

# scrape.run
Scrape urls and save html content. URL csv file should contain two columns containing a url and its corresponding label (the label can be a dummy value such as `""` if scraping unlabeled data). The `Number of parallel requests` parameter will determine how many concurrent open web connections are allowed at one time.

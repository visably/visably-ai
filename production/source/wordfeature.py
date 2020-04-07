#################################################################
# wordfeature.py generate word feature for url classification 
# Visably, LLC retains all rights to this software
# FHS, Jan 15, 2020
#################################################################

import sys
import classify_module as cm
import prm_module as pm
import time
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import spacy

sys.setrecursionlimit(25000)
nlp = spacy.load("en",  disable=["tagger", "parser", "tokenizer", "ner"])

###############################################################################
# Functions go here

###############################################################################  
print('\nwordfeature.py program started ',datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
start_time = time.perf_counter()

# Get parameters filename
prm_fname = 'Dummy'
if len(sys.argv)>1: # if more than one argument passed, assume second argument is parameters filename
    prm_fname=sys.argv[1]
    if len(prm_fname)<4:
        prm_fname = 'Dummy'
if prm_fname[(len(prm_fname)-4):len(prm_fname)] != '.prm': # check if valid parameters filename
    prm_fname = '../prm/wordfeature.prm' # default parameters filename
    
# load word feature parameters
prm_wf = pm.get_prm_wordfeature(prm_fname)

# load wordcount parameters
prm_wc = pm.get_prm_wordcount(prm_wf['wordcount_prm_fname'])

# load URLs
urls = cm.loadurls(prm_wf)

# load text word count file
fullfilename = prm_wc['wordcount_toutput_fname'] 
fwt = pd.read_csv(fullfilename,nrows=prm_wf['max_textwordfeatures'],header=0)
print('\nLoaded text word feature list file ','"' + prm_wc['wordcount_toutput_fname'] + '"',' with ', 
      fwt.shape[0],' rows and ', fwt.shape[1],' columns.')
fwt = fwt.set_index('word')

# load link word count file
fullfilename = prm_wc['wordcount_loutput_fname'] 
fwl = pd.read_csv(fullfilename,nrows=prm_wf['max_linkwordfeatures'],header=0)
print('\nLoaded link word feature list file ','"' + prm_wc['wordcount_loutput_fname'] + '"',' with ', 
      fwl.shape[0],' rows and ', fwl.shape[1],' columns.')
fwl = fwl.set_index('word')

# load htmltag word count file
fullfilename = prm_wc['wordcount_houtput_fname'] 
fwh = pd.read_csv(fullfilename,nrows=prm_wf['max_htmltagwordfeatures'],header=0)
print('\nLoaded htmltag word feature list file ','"' + prm_wc['wordcount_houtput_fname'] + '"',' with ', 
      fwh.shape[0],' rows and ', fwh.shape[1],' columns.')
fwh = fwh.set_index('word')

# create complete list of feature names
cnames = cm.featurenames(urls,fwt,fwl,fwh,prm_wf)

def get_features(iurl):
    current_url = urls.iloc[iurl,urls.columns.get_loc('url')]
    if 'label' in urls.columns:
        current_label = urls.iloc[iurl,urls.columns.get_loc('label')]
    
    fname_location = urls.columns.get_loc('fname')
    current_fname = urls.iloc[iurl,fname_location]

    current_fname = prm_wf['url_directory']+'/'+current_fname
    if prm_wc["verbose"]:
        print('\niurl=',iurl,' url=',current_url, end='')
          
    # set error flag to False
    err_flag = False
    
    # extract html_code from file current_fname
    soup = cm.geturl(current_fname)
    
    if soup != None: # if file load was successful
        current_features = cm.featuremake(soup,fwt,fwl,fwh,current_url,cnames,prm_wc,prm_wf,nlp)

        # If features were created, add to list
        if (len(current_features)>0):
            if prm_wc["verbose"]:
                print('Adding ',len(current_features),' to features_list')
            with lock:
                features_list.append(current_features)
                url_list.append(current_url)
                if 'label' in urls.columns:
                    label_list.append(current_label)
    else:
        err_flag = True

    return err_flag

manager = mp.Manager()

# build empty feature lists
url_list = manager.list()
if 'label' in urls.columns:
    label_list = manager.list()
features_list = manager.list()
lock = mp.Lock()

pool = mp.Pool(processes=mp.cpu_count())
num_errors = 0

# for _ in tqdm(pool.imap_unordered(get_features, urls.index, chunksize=64), total=len(urls.index)):
for err_flag in tqdm(pool.imap_unordered(get_features, range(len(urls)), chunksize=64), total=len(urls)):
    if err_flag:
        num_errors += 1

pool.close()
pool.join()

print(f"Number of errors: {num_errors}, Percent errors: {num_errors / len(urls):.2f}")

url_list = list(url_list)
if 'label' in urls.columns:
    label_list = list(label_list)
features_list = list(features_list)


# Create data frame with url, label (if available), and features
if 'label' in urls.columns:
    # assert len(cnames) == len(features_list[0]) + 2
    df=pd.concat([pd.DataFrame(url_list),pd.DataFrame(label_list),pd.DataFrame(features_list)], axis=1)
else:
    # assert len(cnames) == len(features_list[0]) + 1
    df=pd.concat([pd.DataFrame(url_list),pd.DataFrame(features_list)], axis=1)

df.columns=cnames

fullfilename = prm_wf['wordfeature_output_fname']            
df.to_csv(fullfilename,index=False,sep="|")
print('\n\nwrote csv word feature file ',prm_wf['wordfeature_output_fname'],' with shape=',df.shape) 

time_taken = time.perf_counter() - start_time
print('\nDone.  Elapsed time=','%.4f' % time_taken,' seconds') 
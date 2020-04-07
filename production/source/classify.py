#################################################################
# classify.py classification from random forest or xgboost model
# Visably, LLC retains all rights to this software
# FHS, Jan 18, 2020
#################################################################

# libraries go here
import pandas as pd
import time
import sys
import prm_module as pm
import classify_module as cm
import statistics as st
import numpy as np
import pickle

###############################################################################
# Functions go here
    
###############################################################################
print('\nProcessing starts here...')
start_time = time.perf_counter()

# Get parameters filename
prm_fname = 'Dummy'
if len(sys.argv)>1: # if more than one argument passed, assume second argument is parameters filename
    prm_fname=sys.argv[1]
    if len(prm_fname)<4:
        prm_fname = 'Dummy'
if prm_fname[(len(prm_fname)-4):len(prm_fname)] != '.prm': # check if valid parameters filename
    prm_fname = '../prm/classify.prm' # default parameters filename

# load parameters
prm = pm.get_prm_classify(prm_fname)

# load the model
fullfilename = prm['model_input_fname']
dbfile = open(fullfilename,'rb')
clf = pickle.load(dbfile)
dbfile.close()
print('\nLoaded model file ','"'+prm['model_input_fname']+'"')

if ('sklearn.ensemble.forest.RandomForestClassifier' in str(type(clf))):
    print('\nRandom Forest Model:')
    print('model fit had oob score =',round(clf.oob_score_,3))
    print('model used ',clf.n_estimators,' trees.')
    print('model expects ',clf.n_features_,' input features.')
    print('produces ',clf.n_classes_,' classification categories:',list(clf.classes_))
elif ('xgboost.sklearn.XGBClassifier' in str(type(clf))): 
    print('\nXGBoost Model:')
    print('model used ',clf.n_estimators,' trees.')
    print('model expects ',clf.feature_importances_.shape[0],' input features.')
    print('produces ',clf.n_classes_,' classification categories:',list(clf.classes_))

# load features file
fullfilename = prm['wordfeature_input_fname']
df = pd.read_csv(fullfilename,header=0,sep="|") 
print('\nLoaded feature file ','"' + prm['wordfeature_input_fname'] + '"',' for classification with ', 
      df.shape[0],' rows and ', df.shape[1],' columns.')

# Prepare features
X,y,urls,dropped_urls = cm.featureprep(df,prm)

# classify urls
y_predict = clf.predict(X)

# votes for url classifications
y_vote = np.amax(clf.predict_proba(X),axis=1)

# Change classifications with vote below threshold to 'None'
y_predict[y_vote<prm['min_vote']] = 'None'
print('\nConverting ',sum(y_vote<prm['min_vote']),' classifications to "None" that have vote value below the ',
      prm['min_vote'],' threshold.')

# add in dropped urls and assign classification label 'None', vote 0
urls = urls.append(dropped_urls)
y_predict = np.append(y_predict,np.array(['None']*len(dropped_urls)))
y_vote = np.append(y_vote,np.array([0]*len(dropped_urls)))

# Produce some statistics
labels = np.unique(y_predict)
cnames = ['label','count','vmin','vmean','vmax']
sumstats = pd.DataFrame([[0]*len(cnames)]*len(labels),columns=cnames)
for i in range(len(labels)):
    label = labels[i]
    sumstats.iloc[i,1]=len(y_predict[y_predict==label])
    sumstats.iloc[i,2]=round(np.amin(y_vote[y_predict==label]),3)
    sumstats.iloc[i,3]=round(st.mean(y_vote[y_predict==label]),3)
    sumstats.iloc[i,4]=round(np.amax(y_vote[y_predict==label]),3)
    
sumstats['label']=labels
print('\n',sumstats)

# Assemble results dataframe
res = pd.DataFrame({'url':urls,
                    'label':y_predict,
                    'vote':y_vote})
res = res[['url','label','vote']]

res = res.sort_values('vote',ascending=False)
res.index = range(len(res))

# Save results
fullfilename = prm['classified_urls_output_fname']            
res.to_csv(fullfilename,index=False)
print('\n\nwrote classified url file ',prm['classified_urls_output_fname'],' with shape=',res.shape) 

time_taken = time.perf_counter() - start_time
print('\nDone.  Elapsed time=','%.4f' % time_taken,' seconds') 
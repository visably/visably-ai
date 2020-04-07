#################################################################
# prm_module.py parms file processing for url classifications 
# Visably, LLC retains all rights to this software
# FHS, Jan 21, 2020
#################################################################

import sys
import pandas as pd
import collections

# Decodes [] bracket delimited text parameters list
def prm_decode(prm_text):
    # input prm_text: text parameters, each list item is 1 line of text
    # output prm_df: dataframe with text content, count of bracket pairs, 
    #    bracket pair contents for up to 'max_vars' (10) variables
    # strip out trailing line feeds
    prm_text = [s.rstrip('\n') for s in prm_text]
    # strip leading and trailing whitespace
    prm_text = [s.strip() for s in prm_text]
    # strip '#' commented sections from text
    for si in range(len(prm_text)):
        if prm_text[si].find('#') >= 0:
            prm_text[si] = prm_text[si][0:prm_text[si].find('#')] 

    # Create empty dataframe with one row per line of text
    max_vars = 10
    cnames = ['text','count']
    prm_df = pd.DataFrame({cnames[0]:prm_text,              # raw text dat
                           cnames[1]:[0]*len(prm_text)})    # bracket pair count
    for i in range(max_vars): # create blank columns for up to max_vars variables
        cnames.append('v'+str(i+1))
        prm_df[cnames[len(cnames)-1]] = [None]*len(prm_text)

    prm_df = prm_df[cnames] # order the columns...
    # loop through the strings
    for si in range(len(prm_text)):
        lbcount = 0 # left bracket count
        # Loop through the string looking for [] brackets 
        for j in range(len(prm_text[si])):
            if prm_text[si][j]=='[':
                lbcount += 1
                if lbcount>max_vars:
                    print('\nFATAL ERROR decoding prm data in line ',si+1,'\n',prm_text[si])
                    print('\nHAS MORE THAN ',max_vars,' [] BRACKETS...')
                    sys.exit()
                start_loc = j+1
            if prm_text[si][j]==']':
                prm_df.iloc[si,1] += 1
                stop_loc = j
                if prm_df.iloc[si,1] != lbcount:
                    print('\nFATAL ERROR decoding prm data in line ',si+1,'\n',prm_text[si])
                    print('\nLEFT AND RIGHT BRACKET COUNTS DO NOT MATCH...')
                    sys.exit()
                prm_df.iloc[si,lbcount+1] = prm_text[si][start_loc:stop_loc].strip()
        if prm_df.iloc[si,1] != lbcount:
                print('\nFATAL ERROR decoding prm data in line ',si+1,'\n',prm_text[si])
                print('\nLEFT AND RIGHT BRACKET COUNTS DO NOT MATCH...')
                sys.exit()    
    return prm_df

# locates prm_text line(s) with key phrase, should find exactly one
def prm_keyword_find(prm_text,key):
    # input prm_text: text parameters, each list item is 1 line of text
    # input key: key phrase for search prm_text
    # output r: row number list for prm_text line(s) containing key phrase
    key_phrase = str(key)
    r = [i for i in range(len(prm_text)) if key_phrase in prm_text[i]]
    if len(r) != 1:
        print('\nPROBLEM decoding prm text, need 1 but found ',len(r),' instances of ','"'+key_phrase+'" in parameters text.')
    return r

# create prm dictionary for standard single variable per line parameters
def create_prm_dic(prm_df,keywords):
    # input prm_df: dataframe with prm contents produced by prm_decode
    # input keywords: parameters variable list of tuples
    # tuple element 1: keyword phrase to be found in prm_text
    # tuple element 2: variable name in dictionary
    # tuple element 3: variable type expected
    #    any: no checking for string between brackets
    #    .xxx: check filenames endings, .csv, .prm, etc
    #    int: check that string is an integer
    #    yn: yes/no response, convert to True/False
    #    cust: custom response(s), for multiple inputs or table of inputs
    
    # initialize parameters dictionary
    error_count=0
    prm = list()
    for p in range(len(keywords)):
        r = prm_keyword_find(prm_df['text'],keywords[p][0])
        if len(r) != 1:
            error_count += 1
        else:
            if len(keywords[p][1])>0:
                if keywords[p][2]=='any':
                    prm.append((keywords[p][1],str(prm_df.iloc[r[0],2])))
                elif keywords[p][2][0]=='.':
                    param1 = str(prm_df.iloc[r[0],2])
                    if param1.find(keywords[p][2]) != len(param1) - len(keywords[p][2]):
                        print('ERROR IN "'+keywords[p][0]+'" PARAMETER, EXPECTING '+keywords[p][2]+' FILE and found "'+param1+'".')
                        error_count += 1
                    else:
                        prm.append((keywords[p][1],param1))
                elif keywords[p][2]=='int':
                    param1 = str(prm_df.iloc[r[0],2])
                    if str.isdigit(param1):
                        prm.append((keywords[p][1],int(param1)))
                    else:
                        print('ERROR IN "'+keywords[p][0]+'" PARAMETER, EXPECTING AN INTEGER, FOUND "'+param1+'"')
                        error_count += 1
                elif keywords[p][2]=='float':
                    param1 = str(prm_df.iloc[r[0],2])
                    try:
                        prm.append((keywords[p][1],float(param1)))
                    except Exception as e:
                        print('ERROR IN "'+keywords[p][0]+'" PARAMETER, EXPECTING A FLOAT, FOUND "'+param1+'"')
                        error_count += 1
                elif keywords[p][2]=='yn':
                    param1 = str(prm_df.iloc[r[0],2]).lower()
                    if param1.find('yes') >= 0:
                        prm.append((keywords[p][1],True))
                    else:
                        prm.append((keywords[p][1],False))
    return prm,error_count

# create scrape variables dictionary from bracket delimited ASCII parms file 
def get_prm_scrape(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('scrape V1.0 Program Parameters 09/06/2019','',''),
                ('Title','title','any'),
                ('Filename for URL list to scrape','url_list_fname','.csv'),
                ('Directory to place scraped URLs','url_directory','any'),
                ('Max URLs to scrape','max_urls','int'),
                ('URL Protocol to force','url_protocol','any'),
                ('Overwrite existing URL file (yes/no)','url_overwrite','yn'),
                ('Min existing URL file size (bytes)','min_url_size','int'),
                ('URL Timeout (seconds)','timeout','int'),
                ('Number of parallel requests','num_parallel_requests', 'int'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm = collections.OrderedDict(prm) 

    # any additional checking and changes go here
    if 'url_protocol' in prm: # insure that url_protocol is either https or http
        if prm['url_protocol'] != 'https' and prm['url_protocol'] != 'http':
            prm['url_protocol'] = 'none'
       
    if 'verbose' in prm:
        if (prm['verbose']):
            print('\nListing of parameters file for scrape.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm
    
# create wordcount variables dictionary from bracket delimited ASCII parms file 
def get_prm_wordcount(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('wordcount V1.0 Program Parameters 01/14/2020','','any'),
                ('Title','title','any'),
                ('Filename for wordcount URL list','url_list_fname','.csv'),
                ('Directory for input URLs','url_directory','any'),
                ('Filename for text wordcount output file','wordcount_toutput_fname','.csv'),
                ('Filename for link wordcount output file','wordcount_loutput_fname','.csv'),
                ('Filename for htmltag wordcount output file','wordcount_houtput_fname','.csv'),
                ('Maximum number of text characters per document', 'max_document_text_characters','int'),
                ('Maximum number of text words per document ', 'max_document_text_words','int'),
                ('Maximum number of link words per document ', 'max_document_link_words','int'),
                ('Maximum number of html tag word features', 'max_document_html_tag_words','int'),
                ('Max URLs for wordcount','max_urls','int'),
                ('Minimum word length','min_word_len','int'),
                ('Maximum word length','max_word_len','int'),
                ('Drop stop words (yes/no)','drop_stop_words','yn'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm_wc, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm_wc = collections.OrderedDict(prm_wc)        
    if 'verbose' in prm_wc:
        if (prm_wc['verbose']):
            print('\nListing of parameters file for wordcount.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm_wc

# create wordfeature variables dictionary from bracket delimited ASCII parms file 
def get_prm_wordfeature(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('wordfeature V1.0 Program Parameters 01/15/2020','',''),
                ('Title','title','any'),
                ('Filename wordcount prm file','wordcount_prm_fname','.prm'),
                ('Filename for wordfeature URL list','url_list_fname','.csv'),
                ('Directory for input URLs','url_directory','any'),
                ('Filename for wordfeature output file','wordfeature_output_fname','.csv'),
                ('Max URLs for wordfeature','max_urls','int'),
                ('Maximum number of text word features','max_textwordfeatures','int'),
                ('Maximum number of link word features','max_linkwordfeatures','int'),
                ('Maximum number of html tag word features','max_htmltagwordfeatures','int'),
                ('ASCII features prefix (none to skip)','ascii_features_prefix','any'),
                ('Word Frequency prefix (none to skip)','word_frequency_features_prefix','any'),
                ('Word Pos. Std. prefix (none to skip)','word_positions_stdev_features_prefix','any'),
                ('Word Frequency prefix Link (none to skip)','lword_frequency_features_prefix','any'),
                ('Word Frequency prefix htmltag (none to skip)','hword_frequency_features_prefix','any'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm_wf, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm_wf = collections.OrderedDict(prm_wf)        
    if 'verbose' in prm_wf:
        if (prm_wf['verbose']):
            print('\nListing of parameters file for wordfeature.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm_wf

# create rftrain variables dictionary from bracket delimited ASCII parms file 
def get_prm_rftrain(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('rftrain V1.0 Program Parameters 09/11/2019','',''),
                ('Title','title','any'),
                ('Filename for wordfeature input .csv file','wordfeature_input_fname','.csv'),
                ('Random forest model output .bin file','rfmodel_output_fname','.bin'),
                ('Max training samples','max_samples','int'),
                ('Min ascii count for wordfeature record','min_ascii_count','int'),
                ('Number of random forest trees','ntrees','int'),
                ('Random forest train/test proportion','rf_train_prop','float'),
                ('Random forest train/test iterations','rf_iterations','int'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm = collections.OrderedDict(prm)        
    if 'verbose' in prm:
        if (prm['verbose']):
            print('\nListing of parameters file for rftain.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm

# create rftrain variables dictionary from bracket delimited ASCII parms file 
def get_prm_xgbtrain(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('xgbtrain V1.0 Program Parameters 01/18/2020','',''),
                ('Title','title','any'),
                ('Filename for wordfeature input .csv file','wordfeature_input_fname','.csv'),
                ('Xgboost model output .bin file','xgbmodel_output_fname','.bin'),
                ('Max training samples','max_samples','int'),
                ('Min ascii count for wordfeature record','min_ascii_count','int'),
                ('Xgboost train/test proportion','xgb_train_prop','float'),
                ('Xgboost train/test iterations','xgb_iterations','int'),
                ('Balance training samples by boosting (yes/no)','balancetrainingsamples','yn'),
                ('Verbose (yes/no)','verbose','yn'),
                ('learning_rate (eta)','learning_rate','float'),
                ('max_depth','max_depth','int'),
                ('subsample','subsample','float'),
                ('colsample_bytree','colsample_bytree','float'),
                ('n_estimators','n_estimators','int'),
                ('nround','nround','int'),
                ('random seed','seed','int'),
                ('eval_metric','eval_metric','any'),
                ('objective','objective','any'),
                ('nthread','nthread','int'),
                ('verbose','xgbverbose','int'),]
    # setup parameters dictionary for 'standard' parameters
    prm, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm = collections.OrderedDict(prm)        
    if 'verbose' in prm:
        if (prm['verbose']):
            print('\nListing of parameters file for xgbtain.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm

# create rfclassify variables dictionary from bracket delimited ASCII parms file 
def get_prm_classify(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('classify V1.0 Program Parameters 01/18/2020','',''),
                ('Title','title','any'),
                ('Wordfeature input .csv file','wordfeature_input_fname','.csv'),
                ('Model (RForest or XGBoost)input .bin file','model_input_fname','.bin'),
                ('Classified url list output file','classified_urls_output_fname','.csv'),
                ('Min ascii count for wordfeature record','min_ascii_count','int'),
                ('Min vote for classification','min_vote','float'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm = collections.OrderedDict(prm)        
    if 'verbose' in prm:
        if (prm['verbose']):
            print('\nListing of parameters file for rfclassify.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm

# create rfclassify variables dictionary from bracket delimited ASCII parms file 
def get_prm_RTclassify(fname):
    # input fname: filename for bracket delimited ASCII parameters file
    # output prm: dictionary of parameter variables
    # read text parameters file
    file1 = open(fname,'r')
    prm_text = file1.readlines()
    file1.close()
    # decode text parameters file
    prm_df = prm_decode(prm_text)
    # keywords parameters variables tuples
    keywords = [('RTclassify V1.0 Program Parameters 10/12/2019','',''),
                ('Title','title','any'),
                ('URL list to be classified input .csv file','url_list_input_fname','.csv'),
                ('Classified url list output .csv file','classified_urls_output_fname','.csv'),
                ('URL classification master list .csv file','url_masterlist_input_fname','.csv'),
                ('URL Protocol to force','url_protocol','any'),
                ('URL Timeout (seconds)','timeout','int'),
                ('Filename for text wordcount input .csv file','wordcount_tinput_fname','.csv'),
                ('Filename for link wordcount input .csv file','wordcount_linput_fname','.csv'),
                ('Minimum word length','min_word_len','int'),
                ('Maximum word length','max_word_len','int'),
                ('Drop stop words (yes/no)','drop_stop_words','yn'),
                ('Maximum number of word features','max_wordfeatures','int'),
                ('ASCII features prefix (none to skip)','ascii_features_prefix','any'),
                ('Word Frequency prefix (none to skip)','word_frequency_features_prefix','any'),
                ('Word Pos. Std. prefix (none to skip)','word_positions_stdev_features_prefix','any'),
                ('Word Frequency prefix Link (none to skip)','lword_frequency_features_prefix','any'),
                ('Random forest model input .bin file','rfmodel_input_fname','.bin'),
                ('Min ascii count for wordfeature record','min_ascii_count','int'),
                ('Min rfvote for classification','min_rfvote','float'),
                ('Verbose (yes/no)','verbose','yn')]
    # setup parameters dictionary for 'standard' parameters
    prm, error_count = create_prm_dic(prm_df, keywords)
    # Any custom parameters go below, multiple parms, parms table, etc.
                    
    prm = collections.OrderedDict(prm)        
    if 'verbose' in prm:
        if (prm['verbose']):
            print('\nListing of parameters file for rfclassify.py\n')
            for i in range(len(prm_df['text'])):
                print(prm_df['text'][i])    
    if error_count>0:
        print('ENCOUNTERED ',error_count,' ERRORS IN PARAMETERS FILE: "'+fname+'"')
        sys.exit()    
    return prm
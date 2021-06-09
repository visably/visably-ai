#################################################################
# classify_module.py support function for url classifications 
# Visably, LLC retains all rights to this software
# FHS, Jan 19, 2020
#################################################################

import pandas as pd
import itertools
from bs4 import BeautifulSoup
# import requests
import re
from nltk.corpus import stopwords
from statistics import stdev
import  asyncio
import aiohttp
from tqdm import tqdm

def loadurls(prm):
    # input  prm - parameters - name of url list .csv file
    #                         - max number of urls to load
    # output urls - pandas dataframe list of urls 
    # load csv file containing list of urls to load, expects header
    fullfilename = prm['url_list_fname']
    urls = pd.read_csv(fullfilename,nrows=prm['max_urls'],header=0)

    print('\nLoaded ','"'+prm['url_list_fname']+'"',' with ', 
          urls.shape[0],' rows and ', urls.shape[1],' columns.')
    if 'label' in urls.columns:    
        label_count = urls.groupby("label").count()
        print('\nURL count by label type\n',label_count)
    else:
        print('\nNo URL labels in file.')

    # create filenames from url names
    urls['fname'] = urls['url']

    def clean_filename(url):
        char_to_lstrip = ['https','http',':','/','w','.']
        for c in char_to_lstrip:
            url = url.lstrip(c)

        char_to_replace = ['/',':','.']
        for c in char_to_replace:
            url = url.replace(c,'_')

        return url

    urls['fname'] = urls['fname'].apply(clean_filename)

    # create filesize variable
    urls['fsize'] = 0

    # create error variable
    urls['error'] = 'None'

    # create load time variable
    urls['time'] = 0

    # if urls don't start with "http://" or "https://" then inserts "http://"
    def add_protocol_if_missing(url):
        if url.find('http://') != 0 and url.find('https://') != 0:
            url = 'http://' + url

        return url

    urls['url'] = urls['url'].apply(add_protocol_if_missing)

    return urls

async def scrapeurl(current_url, current_fname, iurl, prm, session):
    # scrape single url from web
    # input  current_url - url to scrape from web
    #        prm - parameters timeout for url request
    # output html_code - string scraping results
    #        err_flag - True or False
    #        error_code - Message if err_flag True    
    # setup user_agent/headers to mimic browser
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36' 
    headers = {'User-Agent': user_agent} 
    error_code = ''
    html_code = ''
    status_code = -2
    err_flag = False
    try:
        async with session.get(current_url,headers=headers, ssl=False) as page:    
            html_code = await page.text()   # extract html code from page   
            status_code = page.status
            html_code = str(html_code)      
    except Exception as e:
        print('ERROR requesting URL from ',current_url,e) 
        error_code = e
        err_flag = True

    return html_code, err_flag, error_code, status_code, current_url, current_fname, iurl

async def boundscrapeurl(sem, current_url, current_fname, iurl, prm, session):
    async with sem:
        return await scrapeurl(current_url, current_fname, iurl, prm, session)

def geturl(current_fname):
    # input  current_fname - name of previously scraped url file to be loaded from local disk
    # output soup - resulting html code turned into 'soup', None if error
    # load previously scraped url html code from local file
    err_flag = False
    try:
        f = open(current_fname,"r")   
        html_code = f.read()
        f.close()
    except Exception as e:
        print('\nERROR loading html_code from file ',current_fname,e) 
        err_flag = True
    # parse html code
    if err_flag == False:
        try:
            soup = BeautifulSoup(html_code,'html.parser')  #Parse html code
        except Exception as e:
            print('\nERROR parsing URL content from ',current_fname,e)
            err_flag = True
    if err_flag == True:
        soup = None

    return soup

def gettextwords(soup,prm,nlp=None):
    # input  soup - parsed html code from BeautifulSoup
    #        prm - parameters - minimum and maximum word length
    #                           drop stop words, verbose
    # output single_words - list of individual words from soup.get_text()
    # Extract beautiful soop text and convert to lower case words 
    # soup_text = soup.get_text().lower()

    [s.extract() for s in soup(['style', 'script', 'document', 'head', 'meta'])]
    soup_text = soup.getText().lower()

    soup_text = soup_text[:prm['max_document_text_characters']]

    # parse soup_text into words and clean up
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in soup_text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)  
    # split into phrases
    # phrases = list(text.split('\n'))
    phrases = list(text.split(r'\n'))
    char_to_replace = ['<','>','|','&','[',']','@','=','.',';','{','}',':','...','?','!','"','/','+']
    # replace these characters with blank space
    for c in char_to_replace:
        phrases = [p.replace(c,' ') for p in phrases]
    # split phrases into words
    single_words = [re.split(' ',phrase) for phrase in phrases]
    single_words = [w for sublist in single_words for w in sublist] # unroll list of lists or words into single list of words
    # strip any leading or trainling blanks
    single_words = [w.strip() for w in single_words]
    # strip these characters from anywhere in the word
    char_to_delete = ['"',',',')','(','\\s','\\n','\\r','\\t','\\x',"\\'","b'",
                      '0','1','2','3','4','5','6','7','8','9']
    for c in char_to_delete:
        single_words = [w.replace(c,'') for w in single_words] 
    # delete all leading & trailing non-alpha characters
    # char_to_strip = list(range(32,48))+list(range(58,64))+list(range(91,97))+list(range(123,128)) # without digits to be deleted
    char_to_strip = list(range(32,64))+list(range(91,97))+list(range(123,128)) # with digits to be deleted
    char_to_strip = [chr(c) for c in char_to_strip]
    for c in char_to_strip:
        # strip for stripping characters to the right (trailing) and left (leading)
        single_words = [w.strip(c) for w in single_words]
    if prm['verbose']:
        print('   Starting with ',len(single_words),' text words')
    # only keep words that have at least one alpha character
    before_count = len(single_words)
    single_words = [w for w in single_words if any(c.isalpha() for c in w)]
    if prm['verbose']:
        print('   Dropping ',before_count - len(single_words),' text words with no alpha characters.')
    # Drop small and large words
    before_count = len(single_words)
    single_words = [w for w in single_words if len(str(w)) >= prm['min_word_len']]
    if prm['verbose']:
        print('   Dropping ',before_count - len(single_words),' short text words with length less than',prm['min_word_len'])
    before_count = len(single_words)
    single_words = [w for w in single_words if len(str(w)) <= prm['max_word_len']]
    if prm['verbose']:
        print('   Dropping ',before_count - len(single_words),' long text words with length greater than',prm['max_word_len'])
    # Drop stop words
    if prm['drop_stop_words']:
        stop_words = list(set(stopwords.words('english')))
        before_count = len(single_words)
        single_words = [w for w in single_words if w not in stop_words]        
        if prm['verbose']:
            print('   Dropping ',before_count - len(single_words),' stop text words.')

    if nlp != None:
        combined_words = " ".join(single_words)
        if len(combined_words) > 1000000: # Spacy has a maxiumum character limit of 100,000
            combined_words = combined_words[:1000000 - 1]
        try:
            single_words = [token.lemma_ for token in nlp(combined_words)]
            single_words = [w.strip() for w in single_words if len(str(w).strip()) >= prm['min_word_len']]
            single_words = [w.strip() for w in single_words if len(str(w).strip()) <= prm['max_word_len']]
        except:
            print("Spacy nlp failed...")
    if len(single_words) > prm['max_document_text_words']:
        single_words = single_words[:prm['max_document_text_words']]

    return single_words

def getlinkwords(soup,prm):
    # input  soup - parsed html code from BeautifulSoup
    #        prm - parameters - minimum word length
    # output single_linkwords - list of individual words from soup.find_all('a')
    # extract beautiful soup links and convert to lower case words

    souplinks = soup.find_all('a')
    souplinks = souplinks[:prm['max_document_link_words']]

    souplinks = [str(s.text).lower() for s in souplinks]

    # convert to list of list of ascii codes
    ascii_codes = [list(map(ord,sl)) for sl in souplinks]
    
    # set non-characters ascii codes to blank (32)
    for ac in range(len(ascii_codes)):
        for c in range(len(ascii_codes[ac])):
            if ascii_codes[ac][c] < 97 or ascii_codes[ac][c] > 122: # before 'a' or after 'z'
                ascii_codes[ac][c]=32
                
    # return to strings
    souplinks1 = [''.join(map(chr,ac)) for ac in ascii_codes]
    
    # drop some link words
    drop_words=['https','href','http','www','com','class']
    for dw in drop_words:
        souplinks1 = [sl.replace(dw,' ') for sl in souplinks1]

    single_linkwords = [re.split(' ',sl) for sl in souplinks1]
    # unroll list of lists or words into single list of words
    single_linkwords = [w for sublist in single_linkwords for w in sublist] 
    # drop words below minimum length
    single_linkwords = [w for w in single_linkwords if len(str(w)) >= prm['min_word_len']]
    single_linkwords = single_linkwords[:prm['max_document_link_words']]
    
    return single_linkwords

def gethtmltagwords(soup,prm):
    # input  soup - parsed html code from BeautifulSoup
    #        prm - parameters - minimum word length
    # output single_htmltagwords - list of individual tag words from soup.find_all().name
    # extract beautiful soup tag words and convert to lower case words
    single_htmltagwords = [tag.name for tag in soup.find_all()]
    single_htmltagwords = single_htmltagwords[:prm['max_document_html_tag_words']]
    return single_htmltagwords

def featurenames(urls,fwt,fwl,fwh,prm):
    # create feature 'column' names, features named must match those in featuremake
    # input  urls - list of urls - needed to determine if 'label' is in columns
    #        fwt - dataframe of text word vocabulary - vocab list is index
    #        fwl - dataframe of link word vocabulary - vocab list is index
    #        fwh - dataframe of html tag word vocabulary - vocab list is index
    #        prm - parameters - prefixes for word freq, word position stdev, ascii char freq, link word freq
    # output cnames - list of feature names - to be used as column names in feature dataframe
    cnames = ['url']
    if 'label' in urls.columns:
        cnames.append('label')
    cnames.append('tword_count')    #Feature1 total word count
    cnames.append('fword_count')    #Feature2 feature word count
    cnames.append('ufword_count')   #Feature3 unique feature word count
    cnames.append('nfw_url_count')  #Feature4 non features words contained in url count
    cnames.append('fwprop')         #Feature5 proportion of feature words to total words
    cnames.append('http_count')     #Feature6 http link count
    cnames.append('dotcom_count')   #Feature7 count of .com links
    cnames.append('url_img_count')  #Feature8 count of image links in URL
    if prm['word_frequency_features_prefix'] != 'none':
        cnames.extend([prm['word_frequency_features_prefix']+str(w).strip().replace("\n", "") for w in fwt.index])
    if prm['word_positions_stdev_features_prefix'] != 'none':
        cnames.extend([prm['word_positions_stdev_features_prefix']+str(w).strip().replace("\n", "") for w in fwt.index])
    cnames.append('ascii_total') # total ascii character count
    if prm['ascii_features_prefix'] != 'none':
        ascii_features = list(range(32,97))+list(range(123,128)) # Only consider uppercase A-Z
        temp = list(ascii_features)
        for i in range(len(temp)):
            temp[i] = prm['ascii_features_prefix']+str(temp[i])
        cnames.extend(temp)
    if prm['lword_frequency_features_prefix'] != 'none':
        cnames.append('linkw_textw_overlap') #fraction of link words contained in text words)
        cnames.extend([prm['lword_frequency_features_prefix']+str(w).strip().replace("\n", "") for w in fwl.index])
    if prm['hword_frequency_features_prefix'] != 'none':
        cnames.extend([prm['hword_frequency_features_prefix']+str(w).strip().replace("\n", "") for w in fwh.index])
    return cnames

def tfidffeaturenames(urls,word_vectorizer,lword_vectorizer,hword_vectorizer,prm):
    # create feature 'column' names, features named must match those in featuremake
    # input  urls - list of urls - needed to determine if 'label' is in columns
    #        fwt - dataframe of text word vocabulary - vocab list is index
    #        fwl - dataframe of link word vocabulary - vocab list is index
    #        fwh - dataframe of html tag word vocabulary - vocab list is index
    #        prm - parameters - prefixes for word freq, word position stdev, ascii char freq, link word freq
    # output cnames - list of feature names - to be used as column names in feature dataframe
    cnames = ['url']
    if 'label' in urls.columns:
        cnames.append('label')
    cnames.append('tword_count')    #Feature1 total word count
    cnames.append('fword_count')    #Feature2 feature word count
    cnames.append('ufword_count')   #Feature3 unique feature word count
    cnames.append('nfw_url_count')  #Feature4 non features words contained in url count
    cnames.append('fwprop')         #Feature5 proportion of feature words to total words
    cnames.append('http_count')     #Feature6 http link count
    cnames.append('dotcom_count')   #Feature7 count of .com links
    cnames.append('url_img_count')  #Feature8 count of image links in URL
    if prm['word_frequency_features_prefix'] != 'none':
        cnames.extend([prm['word_frequency_features_prefix']+str(w.strip()) for w in word_vectorizer.get_feature_names()])
    cnames.append('ascii_total') # total ascii character count
    if prm['ascii_features_prefix'] != 'none':
        ascii_features = list(range(32,97))+list(range(123,128)) # Only consider uppercase A-Z
        temp = list(ascii_features)
        for i in range(len(temp)):
            temp[i] = prm['ascii_features_prefix']+str(temp[i])
        cnames.extend(temp)
    if prm['lword_frequency_features_prefix'] != 'none':
        cnames.append('linkw_textw_overlap') #fraction of link words contained in text words)
        cnames.extend([prm['lword_frequency_features_prefix']+str(w.strip()) for w in lword_vectorizer.get_feature_names()])
    if prm['hword_frequency_features_prefix'] != 'none':
        cnames.extend([prm['hword_frequency_features_prefix']+str(w.strip()) for w in hword_vectorizer.get_feature_names()])
    return cnames

def featuremake(soup,fwt,fwl,fwh,current_url,cnames,prm_wc,prm_wf,nlp=None):
    # create features for current url from soup, must match columns in featurenames
    # input  soup - parsed html code for current url from BeautifulSoup
    #        fwt - dataframe of text word vocabulary - vocab list is index
    #        fwl - dataframe of link word vocabulary - vocab list is index
    #        current_url - list of text words in url - needed to calculate urlword_count
    #        cnames - feature names - needed for ascii char freq calcs
    #        prm_wc - parameters from wordcount - used for obtaining textwords and linkwords lists
    #        prm_wf - parameters from wordfeature - used for word freq, word position stdev, ascii char freq, link word freq 
    # output current_features - list of features for current url
    # determine textwords from soup
    all_textwords = gettextwords(soup,prm_wc,nlp)

    current_features = list()
  
    if len(all_textwords)>0:
        tword_count = len(all_textwords)
        #Feature1 tword_count
        current_features.append(tword_count) 

        # calculate ufword_count and fword_count
        set_all_textwords = set(all_textwords)
        set_fwt = set(fwt.index)
        set_f_textwords = set_all_textwords.intersection(set_fwt)
        ufword_count = len(set_f_textwords)
        f_textwords = [w.strip() for w in all_textwords if w in list(set_f_textwords)]
        fword_count = len(f_textwords) 
        #Feature2 fword_count   
        current_features.append(fword_count)
        #Feature3 ufword_count
        current_features.append(ufword_count) #3 ufword_count
        if prm_wc["verbose"]:
            print('   Total words=',tword_count,', feature words=',fword_count,', unique feature words=',ufword_count)
        #Feature4 nfw_url_count - number of occurrences of non feature words in URL
        urlword_count = 0
        wordset = set(list(all_textwords)) # unique words in URL
        wordset = wordset.difference(set(fwt.index)) # subtract feature words
        for word in wordset:
            if word in current_url:
                urlword_count += all_textwords.count(word)
        current_features.append(urlword_count)
        #Feature5 fwprop - proportion of feature words to total words
        if (tword_count>0):
            current_features.append(fword_count/tword_count)
        else:
            current_features.append(0)
        # extracts links
        souplinks = soup.find_all('a')
        souplinks = souplinks[:prm_wc['max_document_link_words']]
        souplinks = [str(s.text) for s in souplinks]

        #Feature6 http_count
        http_count = 0
        for s in souplinks:
            if s.count('http')>0:
                http_count += 1
        current_features.append(http_count)
        #Feature7 dotcom_count
        dotcom_count = 0
        for s in souplinks:
            if s.count('.com')>0:
                dotcom_count += 1
        current_features.append(dotcom_count)
        #Feature 8 url_img_count
        current_features.append(len(soup.select('img')))            
        
        # Relative frequency features
        if prm_wc["verbose"]:
            print('   Building features: ',end='')
        # text work frequencies
        if prm_wf['word_frequency_features_prefix'] != 'none':
            print(len(fwt),prm_wf['word_frequency_features_prefix'],';',end='')
            for feature in fwt.index :
                feature_count = f_textwords.count(feature)
                if fword_count>0 and feature_count>0:
                    current_features.append(feature_count/fword_count) 
                else:
                    current_features.append(0)
        # Standard Deviations of feature word positions
        if prm_wf['word_positions_stdev_features_prefix'] != 'none':
            print(len(fwt),prm_wf['word_positions_stdev_features_prefix'],';',end='')
            for feature in fwt.index :
                if fword_count>0 and all_textwords.count(feature)>0:
                    current_features.append(stdev([index for index,w in enumerate(all_textwords) if w==feature])/tword_count)
                else:
                    current_features.append(0)
        soup_text = soup.get_text().lower()

        current_features.append(len(soup_text)) # ascii_total total ascii character count
        if prm_wf['ascii_features_prefix'] != 'none':
            ascii_codes = list(map(ord,str(soup_text).upper())) # convert all lowercase a-z to A-Z uppercase
            # determine ascii features to count from the cnames determined in featurenames
            ascii_features = [int(f.replace(prm_wf['ascii_features_prefix'],'')) for f in cnames if f.find(prm_wf['ascii_features_prefix'])>=0]
            print(len(ascii_features),prm_wf['ascii_features_prefix'],';',end='')
            for feature in ascii_features:
                current_features.append(ascii_codes.count(feature)/len(ascii_codes))                
        # link word frequencies
        if prm_wf['lword_frequency_features_prefix'] != 'none':
            # determine linkwords from soup
            all_linkwords = getlinkwords(soup,prm_wc)
            # feature link word count
            if (len(all_linkwords)>0):
                set_all_linkwords = set(all_linkwords)
                # determine fraction of link words contained in text words, added January 19, 2020
                set_linkw_in_textw = set_all_linkwords.intersection(set_all_textwords)
                # print('\nDEBUG... found ',len(set_linkw_in_textw),' unique link words matching text words')
                linkw_in_textw = 0
                for w in set_linkw_in_textw:
                    linkw_in_textw += all_linkwords.count(w)
                # print('DEBUG... found ',linkw_in_textw,' total link words matching text words') 
                # print('DEBUG... fraction of total link words ',linkw_in_textw/len(all_linkwords))
                current_features.append(linkw_in_textw/len(all_linkwords))
                                
                set_fwl = set(fwl.index)
                set_fl_textwords = set_all_linkwords.intersection(set_fwl)
                fl_textwords = [w for w in all_linkwords if w in list(set_fl_textwords)]
                flword_count = len(fl_textwords)                
                print(len(fwl),prm_wf['lword_frequency_features_prefix'],';',end='')
                for feature in fwl.index :
                    link_count = fl_textwords.count(feature)
                    if flword_count>0 and link_count>0:
                        current_features.append(link_count/flword_count) 
                    else:
                        current_features.append(0)
            else:
                current_features.extend([0] * len(fwl.index))
        # htmltag word frequencies
        if prm_wf['hword_frequency_features_prefix'] != 'none':
            # determine linkwords from soup
            all_htmltagwords = gethtmltagwords(soup,prm_wc)
            all_htmltagwords = all_htmltagwords[:prm_wc['max_document_html_tag_words']]
            # feature link word count
            if (len(all_htmltagwords)>0):
                set_all_htmltagwords = set(all_htmltagwords)
                set_fwh = set(fwh.index)
                set_fh_textwords = set_all_htmltagwords.intersection(set_fwh)
                fh_textwords = [w.strip() for w in all_htmltagwords if w in list(set_fh_textwords)]
                fhword_count = len(fh_textwords)                
                print(len(fwh),prm_wf['hword_frequency_features_prefix'],';',end='')
                for feature in fwh.index :
                    htmltag_count = fh_textwords.count(feature)
                    if fhword_count>0 and htmltag_count>0:
                        current_features.append(htmltag_count/fhword_count) 
                    else:
                        current_features.append(0)
            else:
                current_features.extend([0] * len(fwh.index))
                    
    return(current_features)

def tfidffeaturemake(soup,word_vectorizer,lword_vectorizer,hword_vectorizer,current_url,cnames,prm_wc,prm_wf,nlp=None):
    # create features for current url from soup, must match columns in featurenames
    # input  soup - parsed html code for current url from BeautifulSoup
    #        fwt - dataframe of text word vocabulary - vocab list is index
    #        fwl - dataframe of link word vocabulary - vocab list is index
    #        current_url - list of text words in url - needed to calculate urlword_count
    #        cnames - feature names - needed for ascii char freq calcs
    #        prm_wc - parameters from wordcount - used for obtaining textwords and linkwords lists
    #        prm_wf - parameters from wordfeature - used for word freq, word position stdev, ascii char freq, link word freq 
    # output current_features - list of features for current url
    # determine textwords from soup
    all_textwords = gettextwords(soup,prm_wc,nlp)
    current_features = list()
  
    if len(all_textwords)>0:
        tword_count = len(all_textwords)
        #Feature1 tword_count
        current_features.append(tword_count) 

        # calculate ufword_count and fword_count
        set_all_textwords = set(all_textwords)
        set_fwt = set(word_vectorizer.get_feature_names())
        set_f_textwords = set_all_textwords.intersection(set_fwt)
        ufword_count = len(set_f_textwords)
        f_textwords = [w.strip() for w in all_textwords if w in list(set_f_textwords)]
        fword_count = len(f_textwords) 
        #Feature2 fword_count   
        current_features.append(fword_count)
        #Feature3 ufword_count
        current_features.append(ufword_count) #3 ufword_count
        if prm_wc["verbose"]:
            print('   Total words=',tword_count,', feature words=',fword_count,', unique feature words=',ufword_count)
        #Feature4 nfw_url_count - number of occurrences of non feature words in URL
        urlword_count = 0
        wordset = set(list(all_textwords)) # unique words in URL
        wordset = wordset.difference(set(word_vectorizer.get_feature_names())) # subtract feature words
        for word in wordset:
            if word in current_url:
                urlword_count += all_textwords.count(word)
        current_features.append(urlword_count)
        #Feature5 fwprop - proportion of feature words to total words
        if (tword_count>0):
            current_features.append(fword_count/tword_count)
        else:
            current_features.append(0)
        # extracts links
        souplinks = soup.find_all('a')
        souplinks = [str(s) for s in souplinks]
        #Feature6 http_count
        http_count = 0
        for s in souplinks:
            if s.count('http')>0:
                http_count += 1
        current_features.append(http_count)
        #Feature7 dotcom_count
        dotcom_count = 0
        for s in souplinks:
            if s.count('.com')>0:
                dotcom_count += 1
        current_features.append(dotcom_count)
        #Feature 8 url_img_count
        current_features.append(len(soup.select('img')))

        tf_idf_vector = word_vectorizer.transform([" ".join(f_textwords)])[0].todense().tolist()[0]            
        
        # Relative frequency features
        if prm_wc["verbose"]:
            print('   Building features: ',end='')
        # text work frequencies
        if prm_wf['word_frequency_features_prefix'] != 'none':
            print(len(word_vectorizer.get_feature_names()),prm_wf['word_frequency_features_prefix'],';',end='')
            for i, feature in enumerate(word_vectorizer.get_feature_names()):
                current_features.append(tf_idf_vector[i])

        # Standard Deviations of feature word positions
        if prm_wf['word_positions_stdev_features_prefix'] != 'none':
            print(len(word_vectorizer.get_feature_names()),prm_wf['word_positions_stdev_features_prefix'],';',end='')
            for feature in fwt.index :
                if fword_count>0 and all_textwords.count(feature)>0:
                    current_features.append(stdev([index for index,w in enumerate(all_textwords) if w==feature])/tword_count)
                else:
                    current_features.append(0)
        soup_text = soup.get_text().lower()

        current_features.append(len(soup_text)) # ascii_total total ascii character count
        if prm_wf['ascii_features_prefix'] != 'none':
            ascii_codes = list(map(ord,str(soup_text).upper())) # convert all lowercase a-z to A-Z uppercase
            # determine ascii features to count from the cnames determined in featurenames
            ascii_features = [int(f.replace(prm_wf['ascii_features_prefix'],'')) for f in cnames if f.find(prm_wf['ascii_features_prefix'])>=0]
            print(len(ascii_features),prm_wf['ascii_features_prefix'],';',end='')
            for feature in ascii_features:
                current_features.append(ascii_codes.count(feature)/len(ascii_codes))                
        # link word frequencies
        if prm_wf['lword_frequency_features_prefix'] != 'none':
            # determine linkwords from soup
            all_linkwords = getlinkwords(soup,prm_wc)
            # feature link word count
            if (len(all_linkwords)>0):
                set_all_linkwords = set(all_linkwords)
                # determine fraction of link words contained in text words, added January 19, 2020
                set_linkw_in_textw = set_all_linkwords.intersection(set_all_textwords)
                # print('\nDEBUG... found ',len(set_linkw_in_textw),' unique link words matching text words')
                linkw_in_textw = 0
                for w in set_linkw_in_textw:
                    linkw_in_textw += all_linkwords.count(w)
                # print('DEBUG... found ',linkw_in_textw,' total link words matching text words') 
                # print('DEBUG... fraction of total link words ',linkw_in_textw/len(all_linkwords))
                current_features.append(linkw_in_textw/len(all_linkwords))
                                
                set_fwl = set(lword_vectorizer.get_feature_names())
                set_fl_textwords = set_all_linkwords.intersection(set_fwl)
                fl_textwords = [w.strip() for w in all_linkwords if w in list(set_fl_textwords)]

                ltf_idf_vector = word_vectorizer.transform([" ".join(fl_textwords)])[0].todense().tolist()[0] 
                flword_count = len(fl_textwords)                
                print(len(lword_vectorizer.get_feature_names()),prm_wf['lword_frequency_features_prefix'],';',end='')
                for i, feature in enumerate(lword_vectorizer.get_feature_names()):
                    current_features.append(ltf_idf_vector[i])
            else:
                current_features.extend([0] * len(lword_vectorizer.get_feature_names()))
        # htmltag word frequencies
        if prm_wf['hword_frequency_features_prefix'] != 'none':
            # determine linkwords from soup
            all_htmltagwords = gethtmltagwords(soup,prm_wc)
            # feature link word count
            if (len(all_htmltagwords)>0):
                set_all_htmltagwords = set(all_htmltagwords)
                set_fwh = set(hword_vectorizer.get_feature_names())
                set_fh_textwords = set_all_htmltagwords.intersection(set_fwh)
                fh_textwords = [w.strip() for w in all_htmltagwords if w in list(set_fh_textwords)]
                htf_idf_vector = word_vectorizer.transform([" ".join(fh_textwords)])[0].todense().tolist()[0] 
                fhword_count = len(fh_textwords)                
                print(len(hword_vectorizer.get_feature_names()),prm_wf['hword_frequency_features_prefix'],';',end='')
                for i, feature in enumerate(hword_vectorizer.get_feature_names()):
                    current_features.append(htf_idf_vector[i])
            else:
                current_features.extend([0] * len(hword_vectorizer.get_feature_names()))
                    
    return(current_features)

def featureprep(df,prm):
    # final feature prep producing X and y just before training or classification
    # input  df - pandas dataframe with calculated features
    #        prm - parameters minimum ascii count - to drop records with insufficient soup text length
    # output X - features pandas dataframe for training/classification
    #        y - labels corresponding to X for training, None if labels not available (classification)
    #        urls - url names corresponding to X and y, pandas series
    #        dropped_urls - url names that were dropped    
    # drop rows without minimum ascii count    
    if "ascii_total" in df.columns.values:
        droprow = [i for i in df.index if df.iloc[i,df.columns.get_loc('ascii_total')]<prm['min_ascii_count']]
        print('\nDropping ',len(droprow),' samples with ascii length less than ',prm['min_ascii_count'])
        X = df.drop(droprow,axis=0)
        dropped_urls = df['url'][droprow]
    else:
        X = df
    print(df.columns.values)
    # create list of non-dropped urls
    urls = X['url']
    # drop non-feature columns
    y = None
    dropcol = ['url']
    # dropcol = ['url','tword_count','fword_count','ufword_count','ascii_total',
    #                      'nfw_url_count','fwprop','http_count','dotcom_count','url_img_count']
    if 'label' in df.columns:
        y = X['label']
        dropcol.append('label')
    
    # drop columns that are not included in features
    X = X.drop(dropcol,axis=1)
     
    print('Dataset has ',X.shape[1],' features and ',X.shape[0],' tagged samples.')
    return X,y,urls,dropped_urls

def samplebalance(X,y,prm):
    # balance training number of samples in each category by boosting smaller sets
    # input  X - feature samples pandas dataframe for training
    #        y - label samples corresponding to X for training
    # output Xboost - boosted and balanced feature samples pandas dataframe for training
    #        yboost - boosted and balanced label samples corresponding to X for training
    Xboost = X
    yboost = y
    yvaluecounts = pd.DataFrame(y.value_counts())
    maxvaluecount = yvaluecounts['label'].max()
    for l in yvaluecounts.index:
        select = y==l
        yselect = y[select]        
        Xselect = X[select]
        if maxvaluecount > len(yselect):
            yboost = yboost.append(yselect.sample(n=maxvaluecount-len(yselect),replace=True,random_state=prm['seed']))
            Xboost = Xboost.append(Xselect.sample(n=maxvaluecount-len(yselect),replace=True,random_state=prm['seed']))
    print('\nBalancing training sample counts by category by boosting, training sample counts:')
    print(pd.DataFrame(yboost.value_counts()))    
    return Xboost,yboost
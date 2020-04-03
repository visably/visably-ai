#################################################################
# scrape.py scrape urls and save html data to text files 
# Visably, LLC retains all rights to this software
# FHS, Oct 10, 2019
#################################################################

import sys
import classify_module as cm
import prm_module as pm
import time
from datetime import datetime
import os.path
from tqdm import tqdm
from collections import Counter
import asyncio
import aiohttp
import multiprocessing as mp
from itertools import repeat

###############################################################################
# Functions go here

###############################################################################

def determine_skip_url(curent_url, current_fname, prm):
    # Check existing file
    scrape_flag = True
    if os.path.exists(current_fname):
        if not prm['url_overwrite'] :
            if os.path.getsize(current_fname) > prm['min_url_size'] :
                scrape_flag = False
        
    return scrape_flag

async def run():
    print('\nscrape.py program started ',datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    start_time = time.perf_counter()

    # Get parameters filename
    prm_fname = 'Dummy'
    if len(sys.argv)>1: # if more than one argument passed, assume second argument is parameters filename
        prm_fname=sys.argv[1]
        if len(prm_fname)<4:
            prm_fname = 'Dummy'
    if prm_fname[(len(prm_fname)-4):len(prm_fname)] != '.prm': # check if valid parameters filename
        prm_fname = '../prm/scrape.prm' # default parameters filename
        
    prm = pm.get_prm_scrape(prm_fname)

    print("Loading urls....")
    # load URLs
    urls = cm.loadurls(prm)

    print("Finished loading urls: ", len(urls))

    print("Now adding protocol")

    def clean_protocol(url):
        if prm['url_protocol'] == "http":
            if url.find("https://") == 0:
                url = url.replace('https', 'http')
                if prm["verbose"]:
                    print('url=',url,' replaced https with http')
        else:
            if url.find("http://") == 0:
                url = url.replace('http', 'https')
                if prm["verbose"]:
                    print('url=',url,' replaced http with https')
        return url

    # insert forced url protocol if selected (not 'none')
    urls['url'] = urls['url'].apply(clean_protocol)

    print("Finished adding protocol")
    # setup user_agent/headers to mimic browser
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36' 
    headers = {'User-Agent': user_agent} 

    skipcount = 0
    status_codes = Counter()
    err_codes = Counter()
    err_flags = Counter()

    sem = asyncio.Semaphore(prm["num_parallel_requests"])
    tasks = []
    conn = aiohttp.TCPConnector(limit=prm["num_parallel_requests"])
    timeout = aiohttp.ClientTimeout(total=prm['timeout'])

    pool = mp.Pool(processes=mp.cpu_count())

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # for iurl in tqdm(range(urls.shape[0])):

        fnames = [prm['url_directory'] + '/' + fname for fname in urls['fname'].tolist()]
        skip_flags = pool.starmap(determine_skip_url, zip(urls["url"].tolist(), fnames, repeat(prm)))

        assert len(skip_flags) == urls.shape[0]

        print("Finished determining skip flags...")

        for iurl in tqdm(range(urls.shape[0])):
            current_url = urls.iloc[iurl,urls.columns.get_loc('url')]
            current_fname = prm['url_directory']+'/'+urls.iloc[iurl,urls.columns.get_loc('fname')]

            scrape_flag = skip_flags[iurl]
            if scrape_flag:
                # set error flag to False
                err_flag = False
                fetch_start_time = time.perf_counter()
                # call function to scrape url
                task = asyncio.ensure_future(cm.boundscrapeurl(sem, current_url, current_fname, iurl, prm, session))
                tasks.append(task)

            else:
                if prm["verbose"]:
                    print('Skipping scrape due to pre-existing file')
                urls.iloc[iurl,urls.columns.get_loc('error')] = 'skipped'
                skipcount += 1



        print(f"Total to process: {len(tasks)}")
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            try:
                html_code, err_flag, err_code, status_code, current_url, current_fname, iurl = await future
                status_codes[status_code] += 1
                err_codes[err_code] += 1
                err_flags[err_flag] += 1
            except Exception as e:
                print(e)
                err_codes[-1] += 1
                status_codes[-1] += 1
                err_flag = True


            # save html_code to file    
            if err_flag==False:
                try:
                    if prm["verbose"]:
                        print('Writing html code to "'+current_fname+'" with ',sys.getsizeof(html_code),' bytes.')

                    urls.iloc[iurl,urls.columns.get_loc('fsize')] = sys.getsizeof(html_code)
                    with open(current_fname,"w") as f:  #open file for writing
                        f.write(html_code) # write html data
                    f.close() # close file
                except Exception as e:
                    print('\nERROR opening/writing file:',current_fname,e)
                    urls.iloc[iurl,urls.columns.get_loc('error')] = e
                    err_flag = True
            urls.iloc[iurl,urls.columns.get_loc('time')] = time.perf_counter() - fetch_start_time


    # Save csv urls file with fname,fsize,error,time
    timestamp = datetime.now().strftime("_%y%m%d_%H%M%S")    
    fullfilename = prm['url_list_fname'].replace('.csv',timestamp+'.csv')
    urls.to_csv(fullfilename)
    print('\nwrote urls csv file ',fullfilename,' with shape=',urls.shape)
    print('\nscraped ',sum(urls['fsize']>0),' out of ',len(urls),' URLs without error, ', 
          sum(urls['fsize']>prm['min_url_size']),' greater than ',prm['min_url_size'],' bytes.')
    if skipcount >0:
        print('skipped ',skipcount,'scrapes due to pre-existing files')

    print("Status codes: ", status_codes)
    print("Error codes: ", err_codes)

    time_taken = time.perf_counter() - start_time
    print('\nDone.  Elapsed time=','%.4f' % time_taken,' seconds') 

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run())
    loop.run_until_complete(future)

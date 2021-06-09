from bs4 import BeautifulSoup, SoupStrainer
import requests
from glob import glob
import numpy as np
from tqdm import tqdm
import random
import pandas as pd
import multiprocessing as mp

def clean_filename(url):
	char_to_lstrip = ['https','http',':','/','w','.']
	for c in char_to_lstrip:
		url = url.lstrip(c)

	char_to_replace = ['/',':','.']
	for c in char_to_replace:
		url = url.replace(c,'_')

	return url
	
def get_links_from_html(html_file):
	with open(html_file) as f:
		html = f.read()
	f.close()
	
	soup = BeautifulSoup(html, "html.parser")
	about_us_links = []
	
	for link in soup.find_all('a', href=True):
		if "about" in link['href'].lower():
			if link['href'] not in about_us_links:
				about_us_links.append(link['href'])
				
	url = html_file.split("/")[-1]

	return url, about_us_links

def get_about_us_links():
	# labeled_data = pd.read_csv("/home/daniel/consulting/visably/visably_data/3_5_2020/ALL_tagged_3_17_20_english_no_protocol_cleaned.csv")
	labeled_data = pd.read_csv("/home/daniel/consulting/visably/visably_data/4_18_2020/whitelist_audit_urls_4_13_20.csv")
	labeled_data["filenames"] = labeled_data["url"].apply(clean_filename)
	cleaned_filenames = labeled_data["filenames"].values
	cleaned_filenames = {x:"" for x in cleaned_filenames}

	htmls = glob(f"/home/daniel/consulting/visably/visably_data/4_18_2020/htmls/*")
	cleaned_htmls = []
	for html in tqdm(htmls):
		if html.split("/")[-1] in cleaned_filenames:
			cleaned_htmls.append(html)
			
			
	htmls = cleaned_htmls

	about_us_dict = {}
	random.shuffle(htmls)

	pool = mp.Pool(processes=mp.cpu_count())

	for output in tqdm(pool.imap_unordered(get_links_from_html, htmls), total=len(htmls)):
		url, about_us_links = output
		about_us_dict[url] = about_us_links

	pool.close()
	pool.join()
	pool.terminate()
				

	for url in about_us_dict:
		about_us_dict[url] = [x for x in about_us_dict[url] if "http" in x]

	link_nums = []
	for url in about_us_dict:
		link_nums.append(len(about_us_dict[url]))

		
	num_0 = sum([1 for x in link_nums if x == 0])
	num_1 = sum([1 for x in link_nums if x == 1])

	print(f"Percent 0: {num_0 / len(link_nums):.2f}")
	print(f"Percent > 0: {1 - num_0 / len(link_nums):.2f}")
	print(f"Percent 1: {num_1 / (len(link_nums)):.2f}")
	print(f"Percent 1 not 0: {num_1 / (len(link_nums) - num_0):.2f}")
	print(f"Average links: {np.mean(link_nums):.1f}")
	print(f"Std links: {np.std(link_nums):.1f}")
	print(f"Max links: {np.max(link_nums):.1f}")

	return about_us_dict

def write_about_us_links(about_us_dict):
	with open("/home/daniel/consulting/visably/visably_data/4_18_2020/about_us_links.csv", "w") as w:
		w.write("url,links\n")
		for url in about_us_dict:
			links_string = "|".join(about_us_dict[url])
			w.write(f"{url},{links_string}\n")

def write_first_about_us_links(about_us_dict):
	with open("/home/daniel/consulting/visably/visably_data/4_18_2020/single_about_us_links.csv", "w") as w:
		w.write("url,label\n")
		num_with_commas = 0
		for url in about_us_dict:
			if len(about_us_dict[url]) == 0:
				continue
			first_link = about_us_dict[url][0]
			if "," in first_link:
				num_with_commas += 1
				continue
			w.write(f'{first_link},""\n')

	print(f"Percent removed with commas: {num_with_commas/len(about_us_dict):.2f}")

if __name__ == "__main__":
	about_us_dict = get_about_us_links()
	write_about_us_links(about_us_dict)
	write_first_about_us_links(about_us_dict)

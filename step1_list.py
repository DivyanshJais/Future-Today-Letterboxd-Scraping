import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import re
import logging

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def extract(tag, strip=True):
    text = tag.get_text(strip=strip) if tag else None
    if text:
        clean_text = text.replace('&nbsp;', ' ')
        return clean_text
    else:
        return None 

def flush_to_disk(buffer_df,output_file):
    temp_df = pd.DataFrame(buffer_df)
    ensure_parent_dir(output_file)
    temp_df.to_csv(
        output_file,
        mode='a',
        header=not os.path.exists(output_file),
        index=False
    )

    buffer_df.clear()

def convert_k_m(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    try:
        if value.endswith('K'):
            return float(value[:-1]) * 1_000
        elif value.endswith('M'):
            return float(value[:-1]) * 1_000_000
        else:
            return float(value.replace(",", ""))
    except:
        return None
    
def load_checkpoint(path):
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None

def mark_completed(checkpoint_path, url):
    ensure_parent_dir(checkpoint_path)
    with open(checkpoint_path, 'w') as f:
        f.write(url)

def list_url_extraction(output_file, checkpoint):
    MAX_LIST_URLS=500
    total_extracted=0
    buffer_df=[]
    LOGGING_FILE='scrap_error.log'
    BATCH_SIZE=100
    MAX_RETRIES=3
    logger = logging.getLogger("step1")
    # logging.basicConfig(
    #     filename=LOGGING_FILE, filemode='a', level=logging.INFO,
    #     format="%(asctime)s | %(levelname)s | %(message)s"
    # )

    headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept-Language":"en-US,en;q=0.9"
    }
    count=0
    BASE_URL='https://letterboxd.com'
    CURR_URL='https://letterboxd.com/lists/popular/this/week/'
    last_completed = load_checkpoint(checkpoint)
    if last_completed:
        CURR_URL = last_completed

    session = requests.Session()
    session.headers.update(headers)
    while CURR_URL:
        print("Scraping the CURR_URL =", CURR_URL)
        logger.info(f"Processing Page URL: {CURR_URL}")
        try:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = session.get(CURR_URL, timeout=10)
                    response.raise_for_status()
                    break
                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt}/{MAX_RETRIES} failed for {CURR_URL}: {e}"
                        )
                    time.sleep(random.uniform(3, 6))
            else:
                logger.error(f"Skipping page after {MAX_RETRIES} failures: {CURR_URL}")
                CURR_URL = None
                continue
            soup=BeautifulSoup(response.text, 'html.parser')
            
            container=soup.find('div', class_='list-summary-list')
            list_items=container.find_all('div', class_='masthead') if container else []
            print(f"Found {len(list_items)} list items on the page.")
            for item in list_items:
                row={'page_url':None,'list_url':None,'list_name':None,'owner_name':None,'film_count':None,'like_count':None,'comment_count':None}
                row['page_url']=CURR_URL
                list_link_tag=item.find('h2', class_='name prettify').find('a')
                list_url=BASE_URL + list_link_tag['href']
                row['list_url']=list_url
                row['list_name']=extract(list_link_tag)
                row['owner_name']=extract(item.find('a', class_='owner'))
                film_text = extract(item.find('span', class_='value'))
                if film_text:
                    film_text = film_text.replace('films', '').replace(',', '').strip()
                    try:
                        row['film_count'] = int(film_text)
                    except ValueError:
                        row['film_count'] = None

                labels=item.find_all('span', class_='label')
                if len(labels)==2:
                    row['like_count']=convert_k_m(extract(labels[0]))
                    row['comment_count']=convert_k_m(extract(labels[1]))
                elif len(labels)==1:
                    row['like_count']=convert_k_m(extract(labels[0]))
                buffer_df.append(row)
                total_extracted+=1
                if total_extracted>=MAX_LIST_URLS:
                    logger.info("Reached MAX_LIST_URLS limit. Stopping Step-1.")
                    print("Reaches 500 list")
                    flush_to_disk(buffer_df, output_file)
                    return
                
            logger.info(f"Completed Page URL: {CURR_URL}")
            mark_completed(checkpoint, CURR_URL)
            next_page_tag=soup.find('a', class_='next')
            if next_page_tag:
                CURR_URL=BASE_URL + next_page_tag['href']
            else:
                CURR_URL=None
        except Exception as e:
            logger.error(f"Error processing {CURR_URL}: {e}")
            CURR_URL=None
            print("Terminating due to error.")
            print(e)
        if len(buffer_df)>=BATCH_SIZE:
            print(f"Flushing {len(buffer_df)} records to disk.")
            flush_to_disk(buffer_df,output_file)
        time.sleep(1)
        count+=1
        if count%50==0:
            session.close()
            session = requests.Session()
            session.headers.update(headers)

    if buffer_df:
        print(f"Flushing remaining {len(buffer_df)} records to disk.")
        flush_to_disk(buffer_df, output_file)
    print("Scraping Completed.")

if __name__ == "__main__":
    OUTPUT_FILE_LIST_URL='Output_list_url.csv'
    CHECKPOINT_URL='list_url_checkpoint.txt'
    list_url_extraction(OUTPUT_FILE_LIST_URL, CHECKPOINT_URL)
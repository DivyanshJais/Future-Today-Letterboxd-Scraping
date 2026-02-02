import numpy as np
import pandas as pd
import os
import requests
import time
import random
from bs4 import BeautifulSoup
import logging
import re
import sys
from playwright.sync_api import sync_playwright

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


def flush_to_disk(buffer_df, output_file):
    temp_df = pd.DataFrame(buffer_df)
    ensure_parent_dir(output_file)
    temp_df.to_csv(
        output_file,
        mode='a',
        header=not os.path.exists(output_file),
        index=False
    )

    buffer_df.clear()

def load_checkpoint(path):
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None

def mark_completed(checkpoint_path, url):
    ensure_parent_dir(checkpoint_path)
    with open(checkpoint_path, 'w') as f:
        f.write(url)

def extract_movie_urls_from_list(input_lists, output_movies, checkpoint):
    USER_DATA_DIR = "browser_profile"
    MAX_MOVIE_PER_LIST=1000
    buffer_df=[]
    logger=logging.getLogger("step2")
    # logging.basicConfig(
    #     filename=LOGGING_FILE, filemode='a', level=logging.INFO,
    #     format="%(asctime)s | %(levelname)s | %(message)s"
    # )
    # headers={
    #     "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    #     "Accept-Language":"en-US,en;q=0.9"
    # }
    count=0
    BASE_URL='https://letterboxd.com'
    CURR_URL=None
    if os.path.exists(input_lists):
        existing_df=pd.read_csv(input_lists)
        list_urls=existing_df['list_url'].drop_duplicates().tolist()
        print(f'Found {len(list_urls)} unique list URLs to process.')
    else:
        print("Please run step1_list.py to generate the list of list URLs first.")
        sys.exit(1)

    if os.path.exists(output_movies):
        print(f'{output_movies} exists. Resuming from checkpoint if available.')
        # seen_movie_urls = set(
        #     pd.read_csv(output_movies, usecols=['movie_url'])['movie_url']
        # )
        
    last_completed = load_checkpoint(checkpoint)
    resume=bool(last_completed)
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=False,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kolkata",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars"
            ]
        )

        browser.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        """)

        page = browser.new_page()

        # IMPORTANT: homepage first
        page.goto("https://letterboxd.com/", wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # Navigate normally
        page.click("a[href='/sign-in/']")
        page.wait_for_timeout(3000)

        input("Log in manually, then press ENTER...")
        browser.close()

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=USER_DATA_DIR,
            headless=False,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kolkata",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars"
            ]
        )

        browser.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        """)

        page = browser.new_page()

        page.goto("https://letterboxd.com/", wait_until="domcontentloaded")
        page.wait_for_timeout(2000)

        for list_url in list_urls:
            movie_count=0
            if resume:
                if list_url != last_completed:
                    continue
                resume = False
                continue
                
            CURR_URL = f"{list_url}"
            print(f"Starting processing for List URL: {list_url}")
            while CURR_URL:
                print(f"Processing List URL: {CURR_URL}")
                logger.info(f"Processing Page URL: {CURR_URL}")
                try:
                    page.goto(CURR_URL, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector("ul.js-list-entries", timeout=8000)

                    html = page.content()
                    if 'js-list-entries' not in html:
                        logger.error(f"Incomplete list page: {CURR_URL}")
                        break

                    soup = BeautifulSoup(html, "html.parser")

                    row={'list_url':None, 'movie_url':None, 'tags':None}
                    row['list_url']=list_url
                    tags=soup.find('ul', class_='tags')
                    if tags:
                        tag_list=[extract(tag) for tag in tags.find_all('li') if extract(tag)]
                        row['tags']=','.join(tag_list)
                    container=soup.find('ul', class_='js-list-entries')
                    li_list=container.find_all('li', class_='posteritem')
                    for li in li_list:
                        react_div=li.select_one('div.react-component[data-item-link]')
                        if not react_div:
                            continue
                        
                        movie_url=BASE_URL + react_div['data-item-link']
                        movie_count+=1
                        # if movie_url in seen_movie_urls:
                        #     continue
                        # seen_movie_urls.add(movie_url)
                        row['movie_url']=movie_url
                        buffer_df.append(row.copy())

                        if movie_count>=MAX_MOVIE_PER_LIST:
                            print(f"Flushing {len(buffer_df)} records after list completion.")
                            flush_to_disk(buffer_df, output_movies)
                            logger.info(
                                f"Reached {MAX_MOVIE_PER_LIST} movies for list {list_url}. Moving to next list."
                                )
                            print(f"Reached {MAX_MOVIE_PER_LIST} movies for list {list_url}. Moving to next list.")
                            break
                    
                    logger.info(f"Completed Page URL: {CURR_URL}")
                    if movie_count >= MAX_MOVIE_PER_LIST:
                        break
                    page_next=soup.find('a', class_='next')
                    if page_next and page_next.has_attr('href'):
                        CURR_URL=BASE_URL+page_next['href']
                    else:
                        CURR_URL=None
                except Exception as e:
                    logger.error(f"Error processing {CURR_URL}: {e}")
                    CURR_URL=None
                    print("Terminating due to error.")
                    print(e)
                count+=1
            mark_completed(checkpoint, list_url)
            if buffer_df:
                print(f"Flushing {len(buffer_df)} records after list completion.")
                flush_to_disk(buffer_df, output_movies)

        if buffer_df:
            print(f"Flushing remaining {len(buffer_df)} records to disk.")
            flush_to_disk(buffer_df, output_movies)
            print("Scraping Completed.")
        browser.close()

if __name__ == "__main__":
    OUTPUT_FILE_LIST_URL='Output_list_url.csv'
    CHECKPOINT_MOVIE_URL='Movie_url_checkpoint.txt'
    OUTPUT_FILE_MOVIE_URL='Output_movie_url.csv'
    extract_movie_urls_from_list(OUTPUT_FILE_LIST_URL, OUTPUT_FILE_MOVIE_URL, CHECKPOINT_MOVIE_URL)
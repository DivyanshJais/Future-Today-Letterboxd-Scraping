import pandas as pd
import os
import logging
import re
import traceback
import sys
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def extract(tag, strip=True):
    if not tag:
        return None
    text = tag.get_text(strip=strip)
    return text.replace("&nbsp;", " ") if text else None

def flush_to_disk(buffer_df, output_file):
    df = pd.DataFrame(buffer_df)
    ensure_parent_dir(output_file)
    df.to_csv(
        output_file,
        mode="a",
        header=not os.path.exists(output_file),
        index=False
    )
    buffer_df.clear()

def load_checkpoint(path):
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None

def mark_completed(path, url):
    ensure_parent_dir(path)
    with open(path, "w") as f:
        f.write(url)

def extract_rating_count(a_tag):
    if not a_tag:
        return None

    title = a_tag.get("data-original-title")
    if not title:
        return None

    m = re.search(r'([\d,]+)', title)
    if not m:
        return None

    return int(m.group(1).replace(',', ''))

def convert_k_m(value):
    if value is None:
        return None
    value = str(value).strip()
    try:
        if value.endswith("K"):
            return float(value[:-1]) * 1_000
        if value.endswith("M"):
            return float(value[:-1]) * 1_000_000
        return float(value.replace(",", ""))
    except:
        return None
    
def fetch_stats(soup, row):
    """
    Attempt to extract watches / lists / likes
    directly from the movie page HTML.
    """

    stats = soup.select("div.production-statistic")

    if not stats:
        # print("[STATS-MAIN] ❌ No production-statistic blocks found")
        return

    for div in stats:
        classes = div.get("class", [])
        label = div.find("span", class_="label")
        value = convert_k_m(extract(label))

        if "-watches" in classes:
            row["movie_watched_by"] = value
        elif "-lists" in classes:
            row["movie_listed_by"] = value
        elif "-likes" in classes:
            row["movie_liked_by"] = value

    # print(
    #     "[STATS-MAIN] ✅",
    #     row["movie_watched_by"],
    #     row["movie_listed_by"],
    #     row["movie_liked_by"]
    # )

def fetch_ratings(soup, row):
    """
    Attempt to extract rating + histogram
    directly from the movie page HTML.
    """

    avg = soup.select_one("span.average-rating")
    if avg:
        row["rating"] = extract(avg)
    # else:
    #     print("[RATINGS-MAIN] ❌ average-rating not found")

    fans = soup.select_one("a.all-link.more-link")
    if fans and "fan" in fans.text.lower():
        row["fans_count"] = convert_k_m(
            extract(fans).replace("fans", "").strip()
        )
    bars=soup.select_one('div.rating-histogram')
    if bars:
        histogram = bars.select_one('ul')
    else:
        return
    if not histogram:
        # print("[RATINGS-MAIN] ❌ rating histogram not found")
        return

    STAR_MAP = {
        "½": "half_stars",
        "★": "one_stars",
        "★½": "one_and_half_stars",
        "★★": "two_stars",
        "★★½": "two_and_half_stars",
        "★★★": "three_stars",
        "★★★½": "three_and_half_stars",
        "★★★★": "four_stars",
        "★★★★½": "four_and_half_stars",
        "★★★★★": "five_stars",
    }

    for li in histogram.select('li.rating-histogram-bar'):
        a = li.select_one('a.bar')
        if not a:
            continue

        title = a.get('data-original-title', '')
        if not title:
            continue
        title = title.replace('half-★', '½')
        m = re.search(r'([\d,]+)', title)
        if not m:
            continue

        count = int(m.group(1).replace(',', ''))

        star_match=re.search(r'([\u00BD★]+)', title)
        if not star_match:
            # print("[RATINGS-MAIN] ❌ Star symbol not found:", title)
            continue
        stars=star_match.group(1)
        # print("[RATINGS-MAIN] Parsed:", stars, count)
        col = STAR_MAP.get(stars)
        if col:
            row[col] = count

    # print("[RATINGS-MAIN] ✅ rating:", row["rating"])

def fetch_imdb_tmdb(duration_text, second_container, row):
    duration_match=re.search(r'(\d+)\s*min', duration_text)
    if duration_match:
        row['duration']=duration_match.group(1)

    for a in second_container.find_all('a', href=True):
        href = a['href']

        if 'imdb.com/title/tt' in href:
            row['imdb'] = href
            m = re.search(r'(tt\d+)', href)
            if m:
                row['imdb_id'] = m.group(1)

        elif 'themoviedb.org/movie/' in href:
            row['tmdb'] = href
            m = re.search(r'/movie/(\d+)', href)
            if m:
                row['tmdb_id'] = m.group(1)

def fetch_info_section(info_section, row, roles_dict, id_name, class_name=None):
    tab=info_section.find('div', id=id_name) if info_section else None
    if tab:
        for h3 in tab.find_all('h3'):
            if class_name:
                span=h3.find('span', class_=class_name)
            else:
                span=h3.find('span')
            if not span:
                continue
            texts=extract(span)
            normalization=None
            for raw, normalized in roles_dict.items():
                if raw.lower() in texts.lower():
                    normalization=normalized
                    break
            if not normalization:
                continue
            people_div=h3.find_next_sibling('div', class_='text-sluglist')
            if not people_div:
                continue
            row[normalization]=', '.join(
                [extract(a) for a in people_div.find_all('a') if extract(a)]
            )

def extract_movie_data(input_movie_urls, output_movie_data, checkpoint):
    buffer_df=[]
    logger=logging.getLogger("step3")
    if not logger.handlers:
        logging.basicConfig(
            filename="logs/step3.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s"
        )
    
    count=0
    BASE_URL='https://letterboxd.com'
    CURR_URL=None
    seen_movie_data=set()
    if os.path.exists(output_movie_data):
        print(f'{output_movie_data} exists. Resuming from checkpoint if available.')
        seen_movie_data = set(
            pd.read_csv(output_movie_data, usecols=['movie_url'])['movie_url']
        )

    if os.path.exists(input_movie_urls):
        existing_df=pd.read_csv(input_movie_urls)
        movie_urls=existing_df['movie_url'].unique().tolist()
        print(f'Found {len(movie_urls)} unique list URLs to process.')
        print(f'{len(movie_urls) - len(seen_movie_data)} is remaining')
    else:
        print("Please run step2_list.py to generate the movie URLs first.")
        sys.exit(1)

    last_completed = load_checkpoint(checkpoint)
    resume=bool(last_completed)
    USER_DATA_DIR = "browser_profile"

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
        context=browser
        page=context.new_page()
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)

        for movie_url in movie_urls:
            if resume:
                if movie_url != last_completed:
                    continue
                resume = False
                continue

            if movie_url in seen_movie_data:
                continue

            CURR_URL=f"{movie_url}"
            print(f'Start Processing Movie URL: {CURR_URL}')
            logger.info(f"Processing {movie_url}")
            try:
                page.goto(movie_url, wait_until="domcontentloaded", timeout=30000)

                try:
                    page.wait_for_selector(
                        "span.average-rating, div.rating-histogram, div.production-statistic",
                        timeout=5000
                    )
                except:
                    print("[MAIN] Rating/stats not visible yet, scraping anyway")

                page.wait_for_timeout(1000)

                html = page.content()

                if 'id="content"' not in html:
                    logger.error(f"Blocked or incomplete HTML for {movie_url}")
                    continue
                soup = BeautifulSoup(page.content(), "html.parser")
                row={'movie_url':CURR_URL, 'title':None, 'release_year':None, 'movie_watched_by':None, 'movie_listed_by':None, 
                'movie_liked_by':None,'tmdb':None, 'imdb':None, 'imdb_id':None, 'tmdb_id':None, 'rating':None, 'duration':None, 
                'actors':None, 'director':None, 'writer':None, 'editor':None, 'cinematography':None, 'producer':None, 
                'composer':None, 'studio':None, 'country':None,'primary_language':None, 'genres':None, 'themes':None, 
                'first_theatrical_release':None, 'OTT_release':None, 'half_stars':None, 'one_stars':None, 'one_and_half_stars':None, 
                'two_stars':None, 'two_and_half_stars':None, 'three_stars':None, 'three_and_half_stars':None,
                'four_stars':None, 'four_and_half_stars':None, 'five_stars':None, 'fans_count':None
                }
                # Title, Release Year, Directors
                first_container=soup.find('div', class_='col-17')
                if first_container:
                    details=first_container.find('div', class_='details')
                else:
                    details=None
                if details:
                    row['title']=extract(details.find('h1', class_='headline-1'))
                    row['release_year']=extract(details.find('span', class_='releasedate'))
                    row['director']=extract(details.find('span', class_='creatorlist'))

                #Duration, IMDB, TMDB
                second_container=soup.find('p', class_='text-link text-footer')
                duration_text=extract(second_container)
                if duration_text:
                    fetch_imdb_tmdb(duration_text, second_container, row)
                
                #actors
                info_section=soup.find('div', id='tabbed-content')
                if not info_section:
                    logger.warning(f"No tabbed-content found for {CURR_URL}")
                else:
                    actors_tab=info_section.find('div', id='tab-cast')
                    if actors_tab:
                        actors=actors_tab.find_all('a', class_='text-slug')
                        row['actors']=', '.join(extract(a) for a in actors[:-1] if extract(a))
                #producers, writers, editors, cinematographers, composers
                TARGET_ROLES={
                    'Writer': 'writer',
                    'Editor': 'editor',
                    'Cinematography': 'cinematography',
                    'Producer': 'producer',
                    'Composer': 'composer'
                }
                crew_name='tab-crew'
                crew_class='crewrole -full'
                fetch_info_section(info_section, row, TARGET_ROLES,crew_name, crew_class)
                
                #studio, countries, primary_language
                TARGET_DETAILS={
                    'Studio':'studio',
                    'Country':'country',
                    'Language':'primary_language'
                }
                detail_name='tab-details'
                fetch_info_section(info_section, row, TARGET_DETAILS,detail_name)
            
                #genres, themes
                TARGET_GENRES={
                    'Genres':'genres',
                    'Themes':'themes'
                }
                genre_name='tab-genres'
                fetch_info_section(info_section, row, TARGET_GENRES,genre_name)
                
                #first_theatrical_release
                TARGET_DATE={
                    'Theatrical':'first_theatrical_release',
                    'Digital':'OTT_release'
                }
                release_tab=info_section.find('div', id='tab-releases') if info_section else None
                if release_tab:
                    for h3 in release_tab.find_all('h3'):
                        if not h3:
                            continue
                        date_text=extract(h3)
                        normalized_date=None
                        for raw, normalized in TARGET_DATE.items():
                            if raw.lower() == date_text.lower():
                                normalized_date=normalized
                                break
                        if not normalized_date:
                            continue
                        div=h3.find_next_sibling('div', class_='release-table -bydate')
                        if not div:
                            continue
                        first_div=div.find('div', class_='listitem')
                        if not first_div:
                            continue
                        date_h5=first_div.find('h5', class_='date')
                        if not date_h5:
                            continue
                        row[normalized_date]=extract(date_h5)
                if not release_tab:
                    logger.warning(f"No releases tab found for {CURR_URL}")
                    
                #Ratings Breakdown
                #fetch_ratings(BASE_URL, soup, row, context)
                fetch_ratings(soup, row)

                #likes, watched, listed
                # fetch_stats(BASE_URL, soup, row, context)
                fetch_stats(soup, row)

                buffer_df.append(row)
                seen_movie_data.add(CURR_URL)
                logger.info(f"Completed Page URL: {CURR_URL}")
                context.set_default_navigation_timeout(45000)
                context.set_default_timeout(30000)
            except Exception as e:
                logger.error(f"Error processing URL: {CURR_URL} with error: {e}")
                print("Terminating due to error.")
                traceback.print_exc()
            count+=1
            if len(buffer_df) >= 100:
                flush_to_disk(buffer_df, output_movie_data)
                mark_completed(checkpoint, movie_url)
                print(f'Flushed data to disk after processing {count} URLs.')
    if buffer_df:
        flush_to_disk(buffer_df, output_movie_data)
        mark_completed(checkpoint, "COMPLETED")
        print(f'Final flush to disk after processing {count} URLs.')
        print('Scraping completed.')

        browser.close()

if __name__ == "__main__":
    CHECKPOINT_MOVIE_DATA='Movie_data_checkpoint.txt'
    OUTPUT_FILE_MOVIE_URL='Output_movie_url.csv'
    OUTPUT_FILE_MOVIE_DATA='Output_movie_data.csv'
    extract_movie_data(OUTPUT_FILE_MOVIE_URL, OUTPUT_FILE_MOVIE_DATA, CHECKPOINT_MOVIE_DATA)
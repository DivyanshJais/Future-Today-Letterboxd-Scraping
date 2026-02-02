# Letterboxd Data Scraping & Integration Pipeline 🎬

A scalable, fault-tolerant web scraping pipeline to extract **Letterboxd lists, movies, and detailed movie metadata** using a hybrid **Requests + Playwright** approach, with checkpointing and relational integrity preserved.

---

## 📌 Project Overview

This project builds a multi-stage scraping pipeline that:
- Extracts large-scale data from Letterboxd
- Preserves **many-to-many relationships** between lists and movies
- Avoids redundant movie data scraping
- Supports crash recovery via checkpoints
- Produces a final analytics-ready dataset

---

## 📂 Repository Structure
```text
.
├── step1_list.py
├── step2_movie_list_playwright.py
├── step3_movie_data_playwright.py
├── step4.py
├── config.py
└── README.md
```

## 🧩 Pipeline Architecture
```
Seed Page
↓
[Step 1] List URL Extraction
↓
[Step 2] Movie URL Extraction (duplicates preserved)
↓
[Step 3] Unique Movie Metadata Scraping
↓
[Step 4] Relational Merge
↓
Final Dataset
```
---

## 🔹 Step 1: List URL Extraction

**File:** `step1_list.py`  
**Technology:** Requests + BeautifulSoup

### Description
- Scrapes up to **500 list URLs** from a Letterboxd discovery page
- Stores results incrementally
- Lightweight step (no JavaScript rendering)

### Output
- `Output_list_url.csv`

### Limitation
- Limited checkpoint support compared to later steps

---

## 🔹 Step 2: Movie URL Extraction per List

**File:** `step2_movie_list_playwright.py`  
**Technology:** Playwright (persistent browser)

### Description
- Visits each list URL
- Extracts **up to 1000 movie URLs per list**
- **Duplicates are intentionally preserved**

### Why duplicates?
If the same movie appears in multiple lists, it is stored once **per list** to maintain relational integrity.

### Output
- `Output_movie_url.csv`

**Columns**
- `list_url`
- `movie_url`
- `tags`

---

## 🔹 Step 3: Movie Metadata Extraction

**File:** `step3_movie_data_playwright.py`  
**Technology:** Playwright + BeautifulSoup

### Description
- Scrapes **only unique movie URLs**
- Uses `networkidle` to ensure dynamic content loads
- Reuses login session via persistent browser profile
- Implements checkpoint-based recovery

### Data Extracted
- Title, release year, duration
- IMDb & TMDB IDs
- Average rating & rating histogram
- Watch, like, list counts
- Cast and crew
- Genres and themes
- Studio, country, language
- Theatrical & digital release dates

### Output
- `Output_movie_data.csv`

---

## 🔹 Step 4: Dataset Merge

**File:** `step4.py`

### Description
Merges all outputs into a single dataset:
```
(list_url, movie_url)
⨝
(movie_url → movie metadata)
⨝
(list_url → list metadata)
```


### Guarantees
- Movie metadata scraped **once**
- Movie appears in final dataset **once per list**
- No redundant scraping

### Output
- `Final_output.csv`

---

## ⚙️ Configuration

**File:** `config.py`

```python
LISTS_URL_CSV = "Output_list_url.csv"
CHECKPOINT_LIST = "List_checkpoint.txt"

MOVIE_LIST_CSV = "Output_movie_url.csv"
CHECKPOINT_MOVIE_URL = "Movie_url_checkpoint.txt"

MOVIE_DATA_CSV = "Output_movie_data.csv"
CHECKPOINT_MOVIE_DATA = "Movie_data_checkpoint.txt"

FINAL_OUTPUT_CSV = "Final_output.csv"

from step1_list import list_url_extraction
from step2_movie_list_playwright import extract_movie_urls_from_list
from step3_movie_data_playwright import extract_movie_data
import config
import logging
import pandas as pd
import os

def setup_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        handler = logging.FileHandler(log_file, mode='a')
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def merge_outputs(list_file, movie_list_file, movie_data_file, final_output_file):
    if not (os.path.exists(list_file) and
            os.path.exists(movie_list_file) and
            os.path.exists(movie_data_file)):
        raise FileNotFoundError("One or more input files missing")

    df_lists = pd.read_csv(list_file)
    df_movie_lists = pd.read_csv(movie_list_file)
    df_movies = pd.read_csv(movie_data_file)

    merged_movies = df_movie_lists.merge(
        df_movies,
        on="movie_url",
        how="left",
        validate="many_to_one" 
    )

    final_df = merged_movies.merge(
        df_lists,
        on="list_url",
        how="left",
        validate="many_to_one"
    )

    final_df.to_csv(final_output_file, index=False)
    logging.info(f"Final merged dataset written to {final_output_file}")


def main():
    # logger1 = setup_logger("step1", "logs/step1.log")
    # list_url_extraction(
    #     output_file=config.LISTS_URL_CSV,
    #     checkpoint=config.CHECKPOINT_LIST
    # )

    # logger2 = setup_logger("step2", "logs/step2.log")
    # extract_movie_urls_from_list(
    #     input_lists=config.LISTS_URL_CSV,
    #     output_movies=config.MOVIE_LIST_CSV,
    #     checkpoint=config.CHECKPOINT_MOVIE_URL
    # )

    # logger3 = setup_logger("step3", "logs/step3.log")
    # extract_movie_data(
    #     input_movie_urls=config.MOVIE_LIST_CSV,
    #     output_movie_data=config.MOVIE_DATA_CSV,
    #     checkpoint=config.CHECKPOINT_MOVIE_DATA
    # )
    merge_outputs(
    list_file=config.LISTS_URL_CSV,
    movie_list_file=config.MOVIE_LIST_CSV,
    movie_data_file=config.MOVIE_DATA_CSV,
    final_output_file=config.FINAL_OUTPUT_CSV
    )


if __name__ == "__main__":
    main()

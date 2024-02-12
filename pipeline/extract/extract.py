# this is a script that is based on the code writted in the notebooks directory
# it scrapes the bjjheroes website and extracts the data into a set of parquet files
# and then uploads them to s3
# heres how you would execute the script
# python extract.py 10 --output ./ to extract the data and save it to the current directory
# or
# python extract.py --s3 's3_folder_name' 10 to export them to s3

import os
import argparse
from typing import Optional, Tuple, Any
from datetime import datetime

import bs4
import requests  # type: ignore
import pandas as pd
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import aiohttp
import asyncio

SOURCE_HOSTNAME = "https://www.bjjheroes.com"


def get_athletes_from_source(html: str) -> pd.DataFrame:
    """
    This function scrapes the athletes from the bjjheroes website
    :param html: html string of the bjjheroes a-z list of athletes
    :return: a dataframe of the athletes with their name, nickname, and url
    """
    soup = bs4.BeautifulSoup(html, "html.parser")
    result = []
    table = soup.find_all("tr")
    for row in table:
        data = row.find_all("td")
        if data:
            name = f"{data[0].text} {data[1].text}"
            name = name.replace("  ", " ")
            a = dict(
                name=name,
                nickname=data[2].text,
                url=f"{SOURCE_HOSTNAME}{data[0].find('a').get('href')}",
            )
            result.append(a)
    dataframe = pd.DataFrame(result)
    dataframe.index.name = "id"
    return dataframe


def scrape_matches_and_performances(
    athlete_df: pd.DataFrame,
    id_to_html: dict[int, str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    this function parses the athlete pages and extracts the matches and performances
    :param athlete_df: the athletes dataframe, this is modified in place
    :param id_to_html: a mapping of athlete_id to a soup object of the athletes page
    :return: a tuple of the matches, and performances dataframes
    """
    matches_df = pd.DataFrame(
        columns=[
            "id",
            "year",
            "competition",
            "method",
            "stage",
            "weight",
        ]
    )
    matches_df.set_index("id", inplace=True)
    performances_df = pd.DataFrame(columns=["match_id", "athlete_id", "result"])

    def scrape_athlete_page(athlete_id: int, html: str) -> None:
        bs = bs4.BeautifulSoup(html, "html.parser")
        table = bs.find("table", {"class": "table table-striped sort_table"})
        if table is None:
            # this is an athlete that has no recorded matches
            return
        body = table.find("tbody")
        rows = body.find_all("tr")

        for row in rows:
            match_details = row.find_all("td")

            match_id = match_details[0].text
            result = match_details[2].text
            method = match_details[3].text
            competition = match_details[4].text
            weight = match_details[5].text
            stage = match_details[6].text
            year = match_details[7].text

            # check if the opponent name is in the athlete_df:
            opponent_name_cell = match_details[1]
            # check if there is a link to the opponents page
            link = opponent_name_cell.find("a")
            if link is not None:
                opponent_name = link.text
                opponent_url = (
                    f"{SOURCE_HOSTNAME}{opponent_name_cell.find('a').get('href')}"
                )
                opponent_id = athlete_df[athlete_df["url"] == opponent_url].index
            else:
                opponent_name = opponent_name_cell.find("span").text
                opponent_url = ""
                opponent_id = athlete_df[athlete_df["name"] == opponent_name].index
            if opponent_id.empty:
                if opponent_name == "N/A":
                    # pandas parses N/A as NaN so we need to replace
                    # it with a string for when we upload to the database
                    opponent_name = "Unknown"
                print(f"new athlete found: {opponent_name}")
                athlete_df.loc[len(athlete_df)] = [opponent_name, "", opponent_url]

            # check if the match is in the matches_df, the match_id is from another
            # source so we need to check if it is already in the dataframe:
            match = matches_df[matches_df.index == match_id]
            if match.empty:
                matches_df.loc[match_id] = [year, competition, method, stage, weight]
            # add the performance to the performances_df
            performances_df.loc[len(performances_df)] = [match_id, athlete_id, result]

    # for each row in the athletes data frame, scrape the matches and performances
    # making sure that the athlete_id is the index of the dataframe
    for i, html in id_to_html.items():
        scrape_athlete_page(i, html)
    performances_df.index.name = "id"
    return matches_df, performances_df


async def get_athlete_pages(
    athletes_df: pd.DataFrame,
    num_to_scrape: Optional[int] = None,
) -> dict[int, str]:
    """
    This function takes in the number of athletes to scrape and returns a mapping of athlete_id to a soup object of the athletes page
    :param athletes_df: the athletes dataframe with the urls
    :param num_to_scrape: the number of athletes to scrape before stopping, this is used for testing
    :return: a mapping of athlete_id to a string representation of the html for the athlete's page
    """
    pages: dict[int, str] = {}

    async def get_page(
        session: aiohttp.ClientSession,
        url: str,
        athlete_id: int,
    ) -> None:
        async with session.get(url) as response:
            try:
                page = await response.text()
                pages[athlete_id] = page
            except Exception as e:
                print(f"could not scrape {url}")
                print("due to the following error")
                print(e)

    start_time = datetime.now()
    # We split the scraping into chunks of 100 to avoid a timeout error from asyncio
    if num_to_scrape is not None:
        athletes_df = athletes_df.head(num_to_scrape)
    for i in range(0, len(athletes_df), 100):
        step_start_time = datetime.now()
        async with aiohttp.ClientSession() as session:
            tasks = []
            start_time = datetime.now()
            for id_, athlete in athletes_df.iloc[i : i + 100].iterrows():
                id_: int  # type: ignore
                tasks.append(get_page(session, athlete["url"], id_))
                count = id_ + 1
                if num_to_scrape is not None and count == num_to_scrape:
                    break
            await asyncio.gather(*tasks)
        print(f"step {i} took {datetime.now() - step_start_time}")

    print(f"total time: {datetime.now() - start_time}")
    return pages


# here is the main scraping function that takes in the number of athletes to scrape
# and returns the dataframes
def scrape(
    num_to_scrape: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function takes in the number of athletes to scrape and returns the dataframes
    for the athletes, matches, and performances
    :param num_to_scrape: the number of athletes to scrape before stopping, this is used for testing
    :return: a tuple of the athletes, matches, and performances dataframes
    """
    res = requests.get(f"{SOURCE_HOSTNAME}/a-z-bjj-fighters-list")
    athlete_df = get_athletes_from_source(res.text)
    athlete_pages = asyncio.run(get_athlete_pages(athlete_df, num_to_scrape))
    matches_df, performances_df = scrape_matches_and_performances(
        athlete_df, athlete_pages
    )
    return athlete_df, matches_df, performances_df


# the following function uses the output of the above function and sends it to s3
def upload_to_s3(
    athlete_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    performances_df: pd.DataFrame,
    s3_folder: str,
) -> None:
    """
    This function takes in the dataframes and uploads them to s3
    :param athlete_df: the athletes dataframe
    :param matches_df: the matches dataframe
    :param performances_df: the performances dataframe
    :param s3_folder: the s3 folder to upload to
    """
    print("uploading to s3")
    athlete_df.to_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/athlete.parquet"
    )
    matches_df.to_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/match.parquet"
    )
    performances_df.to_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/performance.parquet"
    )
    print("upload complete")


def lambda_handler(event: ALBEvent, context: LambdaContext) -> dict[str, Any]:
    """
    returns s3 folder name that the data was uploaded to
    """
    num_to_scrape = event.get("num_to_scrape")
    s3_folder = event.get("s3_folder")
    if s3_folder is None:
        s3_folder = datetime.now().strftime("%Y-%m-%d")
    athlete_df, matches_df, performances_df = scrape(num_to_scrape)
    upload_to_s3(athlete_df, matches_df, performances_df, s3_folder)
    return {
        "statusCode": 200,
        "body": "upload complete",
        "s3_folder": s3_folder,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="scrape bjjheroes and extract the data"
    )
    parser.add_argument(
        "--s3",
        type=str,
        help="the s3 folder to upload to",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="the output directory",
    )
    parser.add_argument(
        "num_to_scrape",
        type=int,
        nargs="?",
        default=None,
        help="the number of athletes to scrape",
    )
    args = parser.parse_args()
    athlete_df, matches_df, performances_df = scrape(args.num_to_scrape)
    if args.s3:
        upload_to_s3(athlete_df, matches_df, performances_df, args.s3)
    if args.output:
        athlete_df.to_csv(os.path.join(args.output, "athlete.csv"))
        matches_df.to_csv(os.path.join(args.output, "match.csv"))
        performances_df.to_csv(os.path.join(args.output, "performance.csv"))
    else:
        print(athlete_df)
        print(matches_df)
        print(performances_df)

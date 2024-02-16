"""
this is a script that scrapes the bjjheroes website and extracts the data into a set of parquet files
and then uploads them to s3.

heres how you would execute the script on the command line:
python extract.py --output ./
or to upload to s3:
python extract.py --s3 's3_folder_name'
"""

import os
import argparse
from typing import Optional, Any, Dict
from datetime import datetime

import bs4
import requests  # type: ignore
import pandas as pd
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import aiohttp
import asyncio

SOURCE_HOSTNAME = "https://www.bjjheroes.com"


class Scraper:
    def __init__(self, num_to_scrape: Optional[int] = None):
        self.num_to_scrape = num_to_scrape
        self.id_to_html: Dict[int, str] = {}
        self.athlete_df = pd.DataFrame(
            columns=[
                "name",
                "nickname",
                "url",
                "needs_scrape",
            ]
        )
        self.matches_df = pd.DataFrame(
            columns=[
                "id",
                "year",
                "competition",
                "method",
                "stage",
                "weight",
            ]
        )
        self.matches_df.set_index("id", inplace=True)
        self.performances_df = pd.DataFrame(
            columns=[
                "match_id",
                "athlete_id",
                "result",
            ]
        )

    def get_athletes_from_source(self, html: str) -> None:
        """
        This function scrapes the initial list of athletes from the bjjheroes website and populates the
        self.athlete_df attribute.
        :param html: html string of the bjjheroes a-z list of athletes
        """
        soup = bs4.BeautifulSoup(html, "html.parser")
        table = soup.find_all("tr")
        results = []
        for row in table:
            data = row.find_all("td")
            if data:
                name = f"{data[0].text} {data[1].text}"
                name = name.replace("  ", " ")
                a = dict(
                    name=name,
                    nickname=data[2].text,
                    url=f"{SOURCE_HOSTNAME}{data[0].find('a').get('href')}",
                    needs_scrape=True,  # this value is true until the athlete has been scraped
                )
                results.append(a)
        self.athlete_df = pd.DataFrame(results)

    def scrape_athlete_page(self, athlete_id: int, html: str) -> None:
        """
        This function scrapes the matches and performances from the athlete page
        it also adds new athletes to the self.athlete_df attribute, some of which
        will have urls that then need to be scraped
        :param athlete_id: the athlete id as it was in the dataframe
        :param html: the text html of the athlete's page
        """
        bs = bs4.BeautifulSoup(html, "html.parser")
        table = bs.find("table", {"class": "table table-striped sort_table"})
        if table is None:
            # this is an athlete that has no recorded matches
            self.athlete_df.loc[athlete_id, "needs_scrape"] = False
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

            # check if the match is in the matches_df, the match_id is from another
            # source so we need to check if it is already in the dataframe:
            match = self.matches_df[self.matches_df.index == match_id]
            if match.empty:
                self.matches_df.loc[match_id] = [
                    year,
                    competition,
                    method,
                    stage,
                    weight,
                ]
            # add the performance to the performances_df
            self.performances_df.loc[len(self.performances_df)] = [
                match_id,
                athlete_id,
                result,
            ]

            # check if the opponent is in the athlete_df, if not add them:
            opponent_name_cell = match_details[1]
            opponent_name = opponent_name_cell.find("span").text
            opponent_url_element = opponent_name_cell.find("a")
            if opponent_url_element is not None:
                # if there is a link, then we want to use that to find the athlete in the dataframe
                opponent_url = f"{SOURCE_HOSTNAME}{opponent_url_element.get('href')}"
                if self.athlete_df[self.athlete_df["url"] == opponent_url].empty:
                    # if the opponent is not in the dataframe, we add them
                    opponent_id = len(self.athlete_df)
                    self.athlete_df.loc[opponent_id] = [
                        opponent_name,
                        "",
                        opponent_url,
                        True,
                    ]
            else:
                # if there is no link, we use the name string to find the athlete in the dataframe
                if opponent_name.lower() in ["n/a", "na"]:
                    # pandas parses N/A as NaN so we replace it with a string
                    # so we don't violate the non-null constraint
                    opponent_name = "Unknown"
                opponent_id_row = self.athlete_df[
                    self.athlete_df["name"] == opponent_name
                ].index
                if opponent_id_row.empty:
                    # we couldn't find the opponent in the dataframe so we add them
                    opponent_id = len(self.athlete_df)
                    self.athlete_df.loc[opponent_id] = [
                        opponent_name,
                        "",
                        "",
                        False,
                    ]
                else:
                    opponent_id = opponent_id_row[0]
                if result == "W":
                    opponent_result = "L"
                elif result == "L":
                    opponent_result = "W"
                else:
                    opponent_result = "D"
                self.performances_df.loc[len(self.performances_df)] = [
                    match_id,
                    opponent_id,
                    opponent_result,
                ]
        self.athlete_df.loc[athlete_id, "needs_scrape"] = False

    def srape_htmls(self) -> None:
        while self.id_to_html:
            i, html = self.id_to_html.popitem()
            self.scrape_athlete_page(i, html)

    async def get_page(
        self,
        session: aiohttp.ClientSession,
        url: str,
        athlete_id: int,
    ) -> None:
        async with session.get(url) as response:
            try:
                page = await response.text()
                self.id_to_html[athlete_id] = page
            except Exception as e:
                print(f"could not scrape {url}")
                print("due to the following error")
                print(e)
                self.athlete_df.loc[athlete_id, "needs_scrape"] = False

    async def get_athlete_pages(
        self,
    ) -> None:
        """
        This function uses asyncio to scrape the athlete pages in parallel.
        When called, it only only downloads the html as a string and stores it in the id_to_html dictionary.
        It only uses urls from the athlete_df attribute where the needs_scrape column is True.
        """
        athletes_to_scrape = self.athlete_df[self.athlete_df["needs_scrape"]]
        # We split the scraping into chunks of 100 to avoid a timeout error from asyncio
        for i in range(0, len(athletes_to_scrape), 100):
            if self.num_to_scrape is not None and i >= self.num_to_scrape:
                break
            step_start_time = datetime.now()
            async with aiohttp.ClientSession() as session:
                tasks = []
                for id_, athlete in athletes_to_scrape.iloc[i : i + 100].iterrows():
                    tasks.append(self.get_page(session, athlete["url"], id_))
                    count = id_ + 1
                    if self.num_to_scrape is not None and count == self.num_to_scrape:
                        self.athlete_df["needs_scrape"] = False
                        break
                await asyncio.gather(*tasks)
            print(f"step {i} took {datetime.now() - step_start_time}")

    def scrape(
        self,
    ) -> None:
        start_time = datetime.now()
        res = requests.get(f"{SOURCE_HOSTNAME}/a-z-bjj-fighters-list")
        self.get_athletes_from_source(res.text)
        i = 0
        while self.athlete_df["needs_scrape"].any():
            print(
                f"found {self.athlete_df['needs_scrape'].sum()} athletes to scrape, starting round {i}"
            )
            asyncio.run(self.get_athlete_pages())
            print("finished downloading htmls")
            self.srape_htmls()
            print(f"finished scraping round {i}")
            i += 1
        print(f"total time: {datetime.now() - start_time}")
        self.athlete_df.index.name = "id"
        self.athlete_df.drop(columns=["needs_scrape"], inplace=True)


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
    scraper = Scraper(num_to_scrape)
    scraper.scrape()
    s3_folder = event.get("s3_folder")
    if s3_folder is None:
        s3_folder = datetime.now().strftime("%Y-%m-%d")
    upload_to_s3(
        scraper.athlete_df, scraper.matches_df, scraper.performances_df, s3_folder
    )
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
    scraper = Scraper(args.num_to_scrape)
    scraper.scrape()
    if args.s3:
        upload_to_s3(
            scraper.athlete_df, scraper.matches_df, scraper.performances_df, args.s3
        )
    if args.output:
        scraper.athlete_df.to_csv(os.path.join(args.output, "athlete.csv"))
        scraper.matches_df.to_csv(os.path.join(args.output, "match.csv"))
        scraper.performances_df.to_csv(os.path.join(args.output, "performance.csv"))
    else:
        print(scraper.athlete_df)
        print(scraper.matches_df)
        print(scraper.performances_df)

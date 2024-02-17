"""
this is a script that scrapes the bjjheroes website and extracts the data into a set of parquet files
and then uploads them to s3.

heres how you would execute the script on the command line:
python extract.py --output ./
or to upload to s3:
python extract.py --s3 's3_folder_name'
"""

import dataclasses
import os
import argparse
from typing import Optional, Any, Dict, Set, Tuple
from datetime import datetime

import bs4
import pandas as pd
import requests  # type: ignore
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import aiohttp
import asyncio

SOURCE_HOSTNAME = "https://www.bjjheroes.com"


@dataclasses.dataclass(frozen=True)
class Athlete:
    id: int
    name: str
    nickname: str
    url: str

    def to_csv_row(self) -> str:
        return f"{self.id},{self.name},{self.nickname},{self.url}"

    def __hash__(self) -> int:
        return hash((self.id, self.name, self.nickname, self.url))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Athlete):
            return False
        return (
            self.id == other.id
            and self.name == other.name
            and self.nickname == other.nickname
            and self.url == other.url
        )


@dataclasses.dataclass(frozen=True)
class Match:
    id: int
    year: str
    competition: str
    method: str
    stage: str
    weight: str

    def to_csv_row(self) -> str:
        return f"{self.id},{self.year},{self.competition},{self.method},{self.stage},{self.weight}"

    def __hash__(self) -> int:
        return hash(
            (self.id, self.year, self.competition, self.method, self.stage, self.weight)
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Match):
            return False
        return (
            self.id == other.id
            and self.year == other.year
            and self.competition == other.competition
            and self.method == other.method
            and self.stage == other.stage
            and self.weight == other.weight
        )


@dataclasses.dataclass(frozen=True)
class Performance:
    match_id: int
    athlete_id: int
    result: str

    def to_csv_row(self) -> str:
        return f"{self.match_id},{self.athlete_id},{self.result}"

    def __hash__(self) -> int:
        return hash((self.match_id, self.athlete_id, self.result))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Performance):
            return False
        return (
            self.match_id == other.match_id
            and self.athlete_id == other.athlete_id
            and self.result == other.result
        )


class Scraper:
    def __init__(self, num_to_scrape: Optional[int] = None):
        self.num_to_scrape = num_to_scrape

        self.download_queue: Set[Tuple[int, str]] = set()
        self.scrape_queue: Set[Tuple[int, str]] = set()
        self.scrape_iteration: int = 0

        self.url_search: Dict[str, int] = {}
        self.name_search: Dict[str, int] = {}

        self.athletes: Set[Athlete] = set()
        self.matches: Set[Match] = set()
        self.performances: Set[Performance] = set()

    @property
    def athlete_csv(self) -> str:
        header = "id,name,nickname,url"
        rows = [a.to_csv_row() for a in self.athletes]
        return "\n".join([header] + rows)

    @property
    def match_csv(self) -> str:
        header = "id,year,competition,method,stage,weight"
        rows = [m.to_csv_row() for m in self.matches]
        return "\n".join([header] + rows)

    @property
    def performance_csv(self) -> str:
        header = "match_id,athlete_id,result"
        rows = [p.to_csv_row() for p in self.performances]
        return "\n".join([header] + rows)

    def upload_to_s3(self, s3_folder: str) -> None:
        athlete_columns = ["id", "name", "nickname", "url"]
        athlete_df = pd.DataFrame(
            [[a.id, a.name, a.nickname, a.url] for a in self.athletes],
            columns=athlete_columns,
        )
        match_columns = ["id", "year", "competition", "method", "stage", "weight"]
        match_df = pd.DataFrame(
            [
                [m.id, m.year, m.competition, m.method, m.stage, m.weight]
                for m in self.matches
            ],
            columns=match_columns,
        )
        performance_columns = ["match_id", "athlete_id", "result"]
        performance_df = pd.DataFrame(
            [[p.match_id, p.athlete_id, p.result] for p in self.performances],
            columns=performance_columns,
        )
        athlete_df.to_parquet(
            f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/athlete.parquet"
        )
        match_df.to_parquet(
            f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/match.parquet"
        )
        performance_df.to_parquet(
            f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/performance.parquet"
        )

    def output_to_csv(self, output_dir: str) -> None:
        with open(os.path.join(output_dir, "athlete.csv"), "w") as f:
            f.write(self.athlete_csv)
        with open(os.path.join(output_dir, "match.csv"), "w") as f:
            f.write(self.match_csv)
        with open(os.path.join(output_dir, "performance.csv"), "w") as f:
            f.write(self.performance_csv)

    def add_athlete(self, athlete: Athlete) -> None:
        """
        This should be the only place that a url is added to the download queue
        otherwise we might end up in an infinite loop
        :param athlete:
        :return:
        """
        self.athletes.add(athlete)
        if athlete.url:
            self.url_search[athlete.url] = athlete.id
            if self.num_to_scrape is None or len(self.athletes) <= self.num_to_scrape:
                self.download_queue.add((athlete.id, athlete.url))
        else:
            self.name_search[athlete.name] = athlete.id

    def get_athletes_from_source(self, html: str) -> None:
        """
        This function scrapes the initial list of athletes from the bjjheroes website
        :param html: html string of the bjjheroes a-z list of athletes
        """
        soup = bs4.BeautifulSoup(html, "html.parser")
        table = soup.find_all("tr")
        for rowNumber, row in enumerate(table):
            data = row.find_all("td")
            if data:
                name = f"{data[0].text} {data[1].text}"
                name = name.replace("  ", " ")
                self.add_athlete(
                    Athlete(
                        id=rowNumber,
                        name=name,
                        nickname=data[2].text,
                        url=f"{SOURCE_HOSTNAME}{data[0].find('a').get('href')}",
                    )
                )

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
            self.matches.add(
                Match(
                    id=match_id,
                    year=year,
                    competition=competition,
                    method=method,
                    stage=stage,
                    weight=weight,
                )
            )
            # add the performance to the performances_df
            self.performances.add(
                Performance(
                    match_id=match_id,
                    athlete_id=athlete_id,
                    result=result,
                )
            )
            # check if the opponent has been scraped yet:
            opponent_name_cell = match_details[1]
            opponent_name = opponent_name_cell.find("span").text
            opponent_url_element = opponent_name_cell.find("a")
            if opponent_url_element is not None:
                # if there is a link, then we want to use that to find the athlete in the dataframe
                opponent_url = f"{SOURCE_HOSTNAME}{opponent_url_element.get('href')}"
                opponent_id = self.url_search.get(opponent_url)
                if opponent_id is None:
                    opponent_id = len(self.athletes)
                    self.add_athlete(
                        Athlete(
                            id=opponent_id,
                            name=opponent_name,
                            nickname="",
                            url=opponent_url,
                        )
                    )
            else:
                opponent_id = self.name_search.get(opponent_name)
                if opponent_id is None:
                    # we have not scraped this athlete yet
                    opponent_id = len(self.athletes)
                    self.add_athlete(
                        Athlete(
                            id=opponent_id,
                            name=opponent_name,
                            nickname="",
                            url="",
                        )
                    )
                # we have to reverse the result for the opponent
                if result == "W":
                    opponent_result = "L"
                elif result == "L":
                    opponent_result = "W"
                else:
                    opponent_result = "D"
                self.performances.add(
                    Performance(
                        match_id=match_id,
                        athlete_id=opponent_id,
                        result=opponent_result,
                    )
                )

    def scrape_htmls(self) -> None:
        start_time = datetime.now()
        while self.scrape_queue:
            remaining = len(self.scrape_queue)
            if remaining % 100 == 0:
                print(
                    f"{remaining} athletes left to scrape for scrape {self.scrape_iteration}"
                )
                print(f"elapsed time: {datetime.now() - start_time}")
            i, html = self.scrape_queue.pop()
            self.scrape_athlete_page(i, html)

    async def get_page(
        self,
        session: aiohttp.ClientSession,
        athlete_id: int,
        url: str,
    ) -> None:
        async with session.get(url) as response:
            try:
                page = await response.text()
                self.scrape_athlete_page(athlete_id, page)
            except Exception as e:
                print(f"could not download page {url}")
                print("due to the following error")
                print(e)

    async def get_athlete_pages(
        self,
    ) -> None:
        """
        This function uses asyncio to scrape the athlete pages in parallel.
        When called, it only only downloads the html as a string and stores it in the id_to_html dictionary.
        It only uses urls from the athlete_df attribute where the needs_scrape column is True.
        """
        # We split the scraping into chunks of 100 to avoid a timeout error from asyncio
        step = 0
        total_steps = len(self.download_queue) // 100
        while self.download_queue:
            step_start_time = datetime.now()
            async with aiohttp.ClientSession() as session:
                tasks = []
                for i in range(100):
                    if not self.download_queue:
                        break
                    id_, url = self.download_queue.pop()
                    tasks.append(self.get_page(session, id_, url))
                await asyncio.gather(*tasks)
            print(
                f"scrape {self.scrape_iteration}, download step {step}/{total_steps} took {datetime.now() - step_start_time}"
            )
            step += 1

    def scrape(
        self,
    ) -> None:
        start_time = datetime.now()
        res = requests.get(f"{SOURCE_HOSTNAME}/a-z-bjj-fighters-list")
        self.get_athletes_from_source(res.text)
        self.scrape_iteration = 0
        while self.download_queue:
            print(
                f"found {len(self.download_queue)} athletes to scrape, starting scrape {self.scrape_iteration}"
            )
            asyncio.run(self.get_athlete_pages())
            print(f"finished scrape {self.scrape_iteration}")
            self.scrape_iteration += 1
        print(f"total time: {datetime.now() - start_time}")


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
    scraper.upload_to_s3(s3_folder)
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
        scraper.upload_to_s3(args.s3)
    if args.output:
        scraper.output_to_csv(args.output)
    else:
        print(scraper.athlete_csv)
        print(scraper.match_csv)
        print(scraper.performance_csv)

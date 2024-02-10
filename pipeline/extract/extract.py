# this is a script that is based on the code writted in the notebooks directory
# it scrapes the bjjheroes website and extracts the data into a set of parquet files
# and then uploads them to s3
# heres how you would execute the script
# python extract.py 10 to export them to local csv files
# or
# python extract.py --s3 's3_folder_name' 10 to export them to s3

import os
import typing

import bs4
import requests
import pandas as pd


def get_athletes_from_source() -> pd.DataFrame:
    result = []
    res = requests.get("https://www.bjjheroes.com/a-z-bjj-fighters-list")
    soup = bs4.BeautifulSoup(res.text, 'html.parser')
    table = soup.find_all('tr')
    for row in table:
        data = row.find_all('td')
        if data:
            name = f"{data[0].text} {data[1].text}"
            name = name.replace("  ", " ")
            a = dict(
                name=name,
                nickname=data[2].text,
                url=f"https://www.bjjheroes.com{data[0].find('a').get('href')}",
            )
            result.append(a)
    dataframe = pd.DataFrame(result)
    dataframe.index.name = "id"
    return dataframe


def scrape_matches_and_performances(
        num_to_scrape: typing.Optional = 10
) -> typing.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function scrapes the matches and performances from each athletes page
    :param num_to_scrape: the number of athletes to scrape
    :return: a tuple of 3 dataframes, the first is the athletes dataframe, the second is the matches dataframe
    and the third is the performances dataframe
    """
    athlete_df = get_athletes_from_source()
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

    def scrape_matches(athlete_id: int, athlete_url: str):
        print(f"scraping match data for {athlete_url}")
        res = requests.get(athlete_url)
        bs = bs4.BeautifulSoup(res.content, features="html.parser")
        table = bs.find("table", {"class": "table table-striped sort_table"})
        if table is None:
            return
        body = table.find("tbody")
        rows = body.find_all('tr')

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
            if opponent_name_cell.find("a"):
                opponent_name = opponent_name_cell.find("a").text
                opponent_url = f"https://www.bjjheroes.com{opponent_name_cell.find('a').get('href')}"
                opponent_id = athlete_df[athlete_df["url"] == opponent_url].index
            else:
                opponent_name = opponent_name_cell.find("span").text
                opponent_url = ""
                opponent_id = athlete_df[athlete_df["name"] == opponent_name].index
            if opponent_id.empty:
                print(f"new athlete found: {opponent_name}")
                if opponent_name == "N/A":
                    continue
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
    for i, row in athlete_df.iterrows():
        scrape_matches(i, row["url"])
        if i == num_to_scrape:
            break
    performances_df.index.name = "id"

    return athlete_df, matches_df, performances_df

# the following function uses the output of the above function and sends it to s3
def upload_to_s3(
        athlete_df: pd.DataFrame,
        matches_df: pd.DataFrame,
        performances_df: pd.DataFrame,
        s3_folder: str
) -> None:
    """
    This function takes in the dataframes and uploads them to s3
    :param athlete_df: the athletes dataframe
    :param matches_df: the matches dataframe
    :param performances_df: the performances dataframe
    :param s3_folder: the s3 folder to upload to
    """
    athlete_df.to_parquet(f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/athlete.parquet")
    matches_df.to_parquet(f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/match.parquet")
    performances_df.to_parquet(f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/performance.parquet")
    print("upload complete")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        num_to_scrape = int(sys.argv[1])
        athlete_df, matches_df, performances_df = scrape_matches_and_performances(num_to_scrape)
        if len(sys.argv) == 3 and sys.argv[1] == "--s3":
            s3_folder = sys.argv[2]
            upload_to_s3(athlete_df, matches_df, performances_df, s3_folder)
        else:
            athlete_df.to_csv("athlete.csv")
            matches_df.to_csv("match.csv")
            performances_df.to_csv("performance.csv")
    else:
        print("usage: extract.py num_to_scrape")
        print("or")
        print("extract.py --s3 s3_folder_name num_to_scrape")
        sys.exit(1)



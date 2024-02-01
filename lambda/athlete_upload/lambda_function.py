import dataclasses
import typing
import os
import pprint

import psycopg2
import requests
import bs4


DB_URL = os.getenv("DB_URL")
connection = psycopg2.connect(
    DB_URL,
)

@dataclasses.dataclass
class Athlete:
    name: str
    nickname: str
    url: str

def lambda_handler(event, context):
    def get_urls():
        print("getting urls from db")
        with connection.cursor() as cursor:
            cursor.execute(
                (
                    "SELECT url"
                    " FROM url;"
                ),
            )
            rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_athletes_from_source(athlete_urls: typing.List[str]):
        print("getting data from source")
        result = []

        def row_to_athlete(row: typing.List[bs4.element.Tag]):
            name = f"{data[0].text} {data[1].text}"
            name = name.replace("  ", " ")
            return Athlete(
                name=name,
                nickname=data[2].text,
                url=f"https://www.bjjheroes.com{data[0].find('a').get('href')}",
            )

        res = requests.get("https://www.bjjheroes.com/a-z-bjj-fighters-list")
        soup = bs4.BeautifulSoup(res.text, 'html.parser')
        table = soup.find_all('tr')
        for row in table:
            data = row.find_all('td')
            if data:
                a = row_to_athlete(data)
                if a.url not in athlete_urls:
                    result.append(a)
        print(f"the following athletes were not found in the database:")
        pprint.pprint(result)
        return result

    def add_athlete_to_db(athlete: Athlete):
        print("adding data to db")
        with connection.cursor() as cursor:
            cursor.execute(
                (
                    "INSERT INTO athlete (name, nickname)"
                    " VALUES (%s, %s)"
                    " RETURNING id"
                ),
                (athlete.name, athlete.nickname),
            )
            id_row = cursor.fetchone()
            id = id_row[0]

            cursor.execute(
                (
                    "INSERT INTO url (url, athlete_id)"
                    " VALUES (%s, %s)"
                ),
                (athlete.url, id),
            )
            connection.commit()

    existing_urls = get_urls()
    athletes = get_athletes_from_source(existing_urls)
    for athlete in athletes:
        print(f"--- adding {athlete.name} to database")
        add_athlete_to_db(athlete)

import os

import psycopg2


DB_URL = os.getenv("DB_URL")
if DB_URL is None:
    raise Exception("You must set the DB_URL environment variable in the lambda function settings")

connection = psycopg2.connect(
    DB_URL,
)

def lambda_handler(event, context):
    scraped_athletes = event
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

    def add_athlete_to_db(athlete: dict):
        print("adding data to db")
        with connection.cursor() as cursor:
            cursor.execute(
                (
                    "INSERT INTO athlete (name, nickname)"
                    " VALUES (%s, %s)"
                    " RETURNING id"
                ),
                (athlete["name"], athlete["nickname"]),
            )
            id_row = cursor.fetchone()
            id = id_row[0]

            cursor.execute(
                (
                    "INSERT INTO url (url, athlete_id)"
                    " VALUES (%s, %s)"
                ),
                (athlete["url"], id),
            )
            connection.commit()

    existing_urls = get_urls()
    for athlete in scraped_athletes:
        # TODO: I'm using the url to detect if the athlete is in the database already, because the owner of the
        #  website might edit the names of the athletes, or there might be multiple athletes with the same name.
        #  There shouldn't be more than one athlete with the same link, though. However, once we start scraping
        #  from multiple sources, this method must be changed, or we will have duplicate athlete entries for each
        #  website we scrape.
        if athlete["url"] not in existing_urls:
            print(f"--- adding {athlete['name']} to database")
            add_athlete_to_db(athlete)

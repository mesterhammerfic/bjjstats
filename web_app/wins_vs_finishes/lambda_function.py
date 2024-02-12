import os
from typing import Tuple, List, Any

import plotly.express as px
from jinja2 import Environment, FileSystemLoader
import psycopg2
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext

DB_URL = os.getenv("DB_URL")
if DB_URL is None:
    raise Exception(
        "You must set the DB_URL environment variable in the lambda function settings"
    )

connection = psycopg2.connect(
    DB_URL,
)
path = os.path.dirname(__file__)
env = Environment(loader=FileSystemLoader(path, encoding="utf8"))


def get_records() -> Tuple[List[str], List[float], List[float]]:
    print("getting urls from db")
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT a.name,
                   a.id,
                   CAST(SUM(CASE
                                WHEN result = 'W' THEN 1
                                ELSE 0
                       END) AS DECIMAL) / COUNT(*) AS win_percent,
                   CAST(SUM(CASE
                                WHEN method LIKE 'Pts:%' THEN 0
                                WHEN method IN ('N/A', 'Points') THEN 0
                                WHEN result = 'W' THEN 1
                                ELSE 0
                       END) AS DECIMAL) /
                   NULLIF(SUM(CASE
                                  WHEN result = 'W' THEN 1
                                  ELSE 0
                       END), 0)                    AS sub_percent
            FROM athlete a
                     JOIN performance p on a.id = p.athlete_id
                     JOIN match m on p.match_id = m.id
            GROUP BY a.name, a.id;
            """,
        )
        rows = cursor.fetchall()
    names = [row[0] for row in rows]
    win_percent = [row[2] for row in rows]
    finish_percent = [row[3] for row in rows]
    return names, win_percent, finish_percent


def handler(event: ALBEvent, context: LambdaContext) -> dict[str, Any]:
    names, win_percent, finish_percent = get_records()

    fig = px.scatter(x=win_percent, y=finish_percent, text=names)

    plotly_jinja_data = {"fig": fig.to_html(full_html=False)}
    # consider also defining the include_plotlyjs parameter to point to an external Plotly.js as described above

    template = env.get_template(
        "wins_vs_finishes.html",
    )
    res = {
        "statusCode": 200,
        "headers": {"Content-Type": "*/*"},
        "body": template.render(plotly_jinja_data),
    }

    return res

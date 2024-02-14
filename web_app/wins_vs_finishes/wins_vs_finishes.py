"""
This script is a lambda function that queries the database and creates a scatter
plot of win percentage vs submission percentage for each athlete. It then uses
Jinja to render the plotly figure into an HTML template and returns the HTML as
the response to the API Gateway request.

You can output the HTML to a file and open it in a browser to see the plot by running
the following code in a local environment:
DB_URL=[SECRET] python wins_vs_finishes.py --output output.html
"""

import os
from typing import Tuple, List, Any
import argparse

import plotly.express as px
from jinja2 import Environment, FileSystemLoader
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import sqlalchemy as sa

DB_URL = os.getenv("DB_URL")
if DB_URL is None:
    raise Exception(
        "You must set the DB_URL environment variable in the lambda function settings"
    )

path = os.path.dirname(__file__)
env = Environment(loader=FileSystemLoader(path, encoding="utf8"))


def get_records_sa() -> Tuple[List[str], List[float], List[float]]:
    """
    This function uses sqlalchemy to get the records from the database
    :return: the names, win_percent, and finish_percent as lists such
    that the ith element of each list corresponds to the ith athlete
    """
    print("getting urls from db")
    sa_engine = sa.create_engine(DB_URL)
    with sa_engine.connect() as conn:
        statement = sa.text(
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
                       END) /
                   NULLIF(SUM(CASE
                                  WHEN result = 'W' THEN 1
                                  ELSE 0
                       END), 0) AS DECIMAL) AS sub_percent
            FROM athlete a
                     JOIN performance p on a.id = p.athlete_id
                     JOIN match m on p.match_id = m.id
            GROUP BY a.name, a.id;
            """
        )
        result = conn.execute(statement)
        rows = result.fetchall()
    names = [row[0] for row in rows]
    win_percent = [row[2] for row in rows]
    finish_percent = [row[3] for row in rows]
    return names, win_percent, finish_percent


def render_html(
    names: List[str], win_percent: List[float], finish_percent: List[float]
) -> str:

    fig = px.scatter(x=win_percent, y=finish_percent, text=names)
    # i'll make the plot resizable so the user can set the width and height
    fig.update_layout(
        autosize=True,
        margin=dict(l=0, r=0, b=0, t=0, pad=0),
    )

    plotly_jinja_data = {"fig": fig.to_html(full_html=False)}
    template = env.get_template(
        "wins_vs_finishes.html",
    )
    string_html: str = template.render(plotly_jinja_data)
    return string_html


def handler(event: ALBEvent, context: LambdaContext) -> dict[str, Any]:
    names, win_percent, finish_percent = get_records_sa()
    html = render_html(names, win_percent, finish_percent)
    res = {
        "statusCode": 200,
        "headers": {"Content-Type": "*/*"},
        "body": html,
    }
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the lambda function locally")
    parser.add_argument(
        "--output",
        type=str,
        default="output.html",
        help="The file to output the html to",
    )
    args = parser.parse_args()
    names, win_percent, finish_percent = get_records_sa()
    html = render_html(names, win_percent, finish_percent)
    with open(args.output, "w") as f:
        f.write(html)
    print(f"HTML written to {args.output}")

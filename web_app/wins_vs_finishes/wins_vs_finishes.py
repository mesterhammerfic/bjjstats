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
from typing import Any, Sequence
import argparse

import plotly.express as px
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import sqlalchemy as sa
from sqlalchemy import Row

DB_URL = os.getenv("DB_URL")
if DB_URL is None:
    raise Exception(
        "You must set the DB_URL environment variable in the lambda function settings"
    )

path = os.path.dirname(__file__)
env = Environment(loader=FileSystemLoader(path, encoding="utf8"))


def get_records() -> Sequence[Row]:
    """
    Get the athlete records from the database
    each row contains the athlete's name, id, wins, subs, total_matches, win percent, and sub percent
    in that order
    """
    sa_engine = sa.create_engine(DB_URL)
    with sa_engine.connect() as conn:
        statement = sa.text(
            """
            with cte as (SELECT a.name,
                                a.id,
                                SUM(CASE
                                        WHEN result = 'W' THEN 1
                                        ELSE 0
                                   END) AS wins,
                                SUM(CASE
                                        WHEN method LIKE 'Pts:%' THEN 0
                                        WHEN method IN ('N/A', 'Points', 'DQ', 'Referee Decision', 'Adv', 'Pen', '---', 'Advantages') THEN 0
                                        WHEN method LIKE 'EBI%' THEN 0
                                        WHEN result = 'W' THEN 1
                                        ELSE 0
                                   END) AS subs,
                                COUNT(*) AS total_matches
                         FROM athlete a
                                  JOIN performance p on a.id = p.athlete_id
                                  JOIN match m on p.match_id = m.id
                         WHERE url != ''
                         GROUP BY a.name, a.id)
            select name, id, wins, subs, total_matches, ROUND(CAST(wins AS DECIMAL) / total_matches * 100, 2) AS win_percent, ROUND(CAST(subs AS DECIMAL) / NULLIF(wins, 0) * 100, 2) AS sub_percent
            from cte
            """
        )
        result = conn.execute(statement)
        rows: Sequence[Row] = result.fetchall()
        if not rows:
            raise Exception("No records found")
    return rows


def render_wins_vs_subs_graph() -> str:
    print("getting records")
    dataframe = pd.DataFrame(
        get_records(),
        columns=[
            "name",
            "id",
            "wins",
            "subs",
            "total_matches",
            "win percent",
            "sub percent",
        ],
    )
    # here i'll case the win and sub percent to floats
    dataframe["win percent"] = dataframe["win percent"].astype(float)
    dataframe["sub percent"] = dataframe["sub percent"].astype(float)
    # fill nulls with 0
    dataframe.fillna(0, inplace=True)
    print("creating figure")
    wins_subs_fig = px.scatter(
        dataframe,
        x="win percent",
        y="sub percent",
        size="wins",
        color="subs",
        hover_data=["name", "total_matches"],
    )
    wins_subs_fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, b=20, t=20, pad=20),
    )
    wins_subs_fig.update_xaxes(title_text="Finish Percentage", range=[0, 105])
    wins_subs_fig.update_yaxes(title_text="Win Percentage", range=[0, 105])
    # here i'll format the hover data to include the name, total matches, win percent, and sub percent
    wins_subs_fig.update_traces(
        hovertemplate="<br>".join(
            [
                "Name: %{customdata[0]}",
                "Total Matches: %{customdata[1]}",
                "Win Percent: %{x}",
                "Sub Percent: %{y}",
            ]
        )
    )
    print("converting plot to html")
    html_fig: str = wins_subs_fig.to_html(full_html=False)
    return html_fig


def create_full_html() -> str:
    wins_vs_subs_graph = render_wins_vs_subs_graph()
    print("rendering full html")
    plotly_jinja_data = {
        "wins_vs_subs_graph": wins_vs_subs_graph,
    }
    template = env.get_template(
        "wins_vs_finishes.html",
    )
    string_html: str = template.render(plotly_jinja_data)
    return string_html


def handler(event: ALBEvent, context: LambdaContext) -> dict[str, Any]:
    res = {
        "statusCode": 200,
        "headers": {"Content-Type": "*/*"},
        "body": create_full_html(),
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
    with open(args.output, "w") as f:
        f.write(create_full_html())
    print(f"HTML written to {args.output}")

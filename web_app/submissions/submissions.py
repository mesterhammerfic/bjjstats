"""
This script is a lambda function that queries the database and creates a scatter
plot of which athletes are the most successful with certain submissions.

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


def get_submission_athlete_data(submission: str) -> Sequence[Row]:
    """
    Get the number of occurrences of each submission type for each athlete
    each row contains the athlete's name, id, wins, method, number of submissions, submissions per win, and win percent
    for example:
    ('Gordon Ryan', 1, 5, 'Armbar', 3, 60.0, 100.0)
    ('Gordon Ryan', 1, 5, 'Choke', 2, 40.0, 100.0)
    ('Dante Leon', 2, 3, 'Armbar', 1, 33.33, 100.0)
    ('Dante Leon', 2, 3, 'Choke', 2, 66.67, 100.0)
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
                                COUNT(*) AS total_matches,
                                SUM(CASE
                                        WHEN result = 'W' and method = :submission THEN 1
                                        ELSE 0
                                    END) AS submissions
                         FROM athlete a
                                  JOIN performance p on a.id = p.athlete_id
                                  JOIN match m on p.match_id = m.id
                         WHERE url != ''
                         GROUP BY a.name, a.id),
                 cte2 as (select a.name, a.id, COUNT(*) as total_submissions
                          from athlete a
                                   join performance p on a.id = p.athlete_id
                                   join match m on p.match_id = m.id
                          where method not like 'Pts:%'
                            and method not IN ('N/A', 'Points', 'DQ', 'Referee Decision', 'Adv', 'Pen', '---', 'Advantages')
                            and method not LIKE 'EBI%'
                            and p.result = 'W'
                          group by a.name, a.id)
            select cte.name,
                   cte.id,
                   cte.wins,
                   cte.submissions,
                   ROUND(cast(cte.wins as decimal) / cte.total_matches * 100, 2)             as win_percent,
                   ROUND(cast(cte.submissions as decimal) / cte.wins * 100, 2) as sub_percent
            from cte
                     join cte2 on cte.id = cte2.id
            where cte.submissions > 1
            and cte.wins > 10
            """
        )
        statement = statement.bindparams(submission=submission)

        result = conn.execute(statement)
        rows: Sequence[Row] = result.fetchall()
        if not rows:
            raise Exception("No records found")
    return rows


def get_submission_data() -> Sequence[Row]:
    sa_engine = sa.create_engine(DB_URL)
    with sa_engine.connect() as conn:
        statement = sa.text(
            """
            select method, COUNT(*) as num_occurrences
            from match m
            where method not like 'Pts:%'
              and method not IN ('N/A', 'Points', 'DQ', 'Referee Decision', 'Adv', 'Pen', '---', 'Advantages')
              and method not LIKE 'EBI%'
            group by method
            having COUNT(*) > 40
            order by method asc
            """
        )
        result = conn.execute(statement)
        rows: Sequence[Row] = result.fetchall()
        if not rows:
            raise Exception("No records found")
    return rows


def render_submission_graph(submission: str) -> str:
    sub_athlete_data = get_submission_athlete_data(submission)
    sub_athlete_df = pd.DataFrame(
        sub_athlete_data,
        columns=[
            "name",
            "id",
            "wins",
            "submissions",
            "win_percent",
            "sub_percent",
        ],
    )
    # create a scatter plot where the x axis is the number of submissions
    # the y axis is the sub percent, and the size and color of the points are the wins
    # as a heat map
    # the hover data is the name, number of submissions, win percent, and total wins
    fig = px.scatter(
        sub_athlete_df,
        x="submissions",
        y="sub_percent",
        size="wins",
        color="wins",
        hover_data=["name", "win_percent", "wins"],
    )

    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, b=20, t=20, pad=20),
    )
    fig.update_xaxes(title_text=f"Number of Wins by {submission}")
    fig.update_yaxes(title_text=f"% of Wins by {submission}")

    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "Name: %{customdata[0]}",
                f"Number of Wins by {submission}" + ": %{x}",
                "Win Percentage: %{customdata[1]}",
                "Total Wins: %{customdata[2]}",
                f"Percentage of Wins by {submission}" + ": %{y}",
            ]
        )
    )
    html_fig: str = fig.to_html(full_html=False)
    return html_fig


def create_full_html(submission: str) -> str:
    submission_graph = render_submission_graph(submission)
    plotly_jinja_data = {
        "submission_graph": submission_graph,
        "submission_list": get_submission_data(),
        "submission": submission,
    }
    template = env.get_template(
        "submissions.html",
    )
    string_html: str = template.render(plotly_jinja_data)
    return string_html


def handler(event: ALBEvent, context: LambdaContext) -> dict[str, Any]:
    submission = event.get("queryStringParameters", {}).get("submission", "Armbar")
    res = {
        "statusCode": 200,
        "headers": {"Content-Type": "*/*"},
        "body": create_full_html(submission),
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
    parser.add_argument(
        "--submission",
        type=str,
        default="Armbar",
        help="The submission to filter the data by",
    )
    args = parser.parse_args()
    submission = args.submission
    with open(args.output, "w") as f:
        f.write(create_full_html(submission))
    print(f"HTML written to {args.output}")

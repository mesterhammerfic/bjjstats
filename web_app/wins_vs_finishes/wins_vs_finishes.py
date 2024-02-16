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
                         WHERE url is not null
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


def get_submission_athlete_data() -> Sequence[Row]:
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
                                COUNT(*) AS total_matches
                         FROM athlete a
                                  JOIN performance p on a.id = p.athlete_id
                                  JOIN match m on p.match_id = m.id
                            WHERE url is not null
                         GROUP BY a.name, a.id),
                 cte2 as (select a.name, a.id, COUNT(*) as num_submissions, method
                          from athlete a
                                   join performance p on a.id = p.athlete_id
                                   join match m on p.match_id = m.id
                          where method not like 'Pts:%'
                            and method not IN ('N/A', 'Points', 'DQ', 'Referee Decision', 'Adv', 'Pen', '---', 'Advantages')
                            and method not LIKE 'EBI%'
                            and p.result = 'W'
                          group by a.name, a.id, method)
            select cte.name, cte.id, cte.wins, cte2.method, cte2.num_submissions, ROUND(cast (cte2.num_submissions as decimal) / nullif(cte.wins, 0) * 100, 2) as sub_per_win, ROUND(cast (cte.wins as decimal) / cte.total_matches * 100, 2) as win_percent
            from cte
                     inner join cte2 on cte.id = cte2.id
            order by cte2.method
            """
        )
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


def render_wins_vs_subs_graph() -> str:
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
    wins_subs_fig = px.scatter(
        dataframe,
        x="win percent",
        y="sub percent",
        hover_data=["name", "total_matches"],
    )
    wins_subs_fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, b=20, t=20, pad=20),
    )
    wins_subs_fig.update_xaxes(title_text="Win Percentage")
    wins_subs_fig.update_yaxes(title_text="Finish Percentage")
    wins_subs_fig.update_xaxes(range=[0, 100])
    wins_subs_fig.update_yaxes(range=[0, 100])
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
    html_fig: str = wins_subs_fig.to_html(full_html=False)
    return html_fig


def render_submission_graph() -> str:
    sub_data = get_submission_data()
    sub_athlete_data = get_submission_athlete_data()
    sub_df = pd.DataFrame(sub_data, columns=["method", "num_occurrences"])
    sub_athlete_df = pd.DataFrame(
        sub_athlete_data,
        columns=[
            "name",
            "id",
            "wins",
            "method",
            "num_submissions",
            "sub_per_win",
            "win_percent",
        ],
    )
    # create a scatter plot figure and add one trace for each of the subs in the sub_df which are invisible until a
    # button is clicked
    fig = px.scatter()
    for method in sub_df["method"]:
        subset = sub_athlete_df[sub_athlete_df["method"] == method]
        data = px.scatter(
            subset,
            x="sub_per_win",
            y="win_percent",
            size="wins",
            color="num_submissions",
            hover_data=["name", "num_submissions", "wins"],
            title=method,
        ).data[0]
        data["name"] = method
        data["visible"] = False
        fig.add_trace(data)
    # add the buttons to click to show the different traces
    buttons = []
    for i, row in sub_df.iterrows():
        method = row.method
        num_occurrences = row.num_occurrences
        buttons.append(
            dict(
                label=f"{method} ({num_occurrences})",
                method="update",
                args=[
                    {"visible": [method == trace.name for trace in fig.data]},
                    {"title": method},
                ],
            )
        )
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.1,
                xanchor="left",
                y=1.1,
                yanchor="top",
            )
        ]
    )
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, b=20, t=20, pad=20),
    )
    fig.update_xaxes(title_text="Percentage of Wins by this Submission", range=[0, 100])
    fig.update_yaxes(title_text="Total Win Percentage", range=[0, 100])

    fig.update_traces(
        hovertemplate="<br>".join(
            [
                "Name: %{customdata[0]}",
                "Number of Submissions: %{customdata[1]}",
                "Percentage of Wins with this Sub: %{x}",
                "Win Percent: %{y}",
                "Total Wins: %{customdata[2]}",
            ]
        )
    )
    html_fig: str = fig.to_html(full_html=False)
    return html_fig


def create_full_html() -> str:
    wins_vs_subs_graph = render_wins_vs_subs_graph()
    submission_graph = render_submission_graph()
    plotly_jinja_data = {
        "wins_vs_subs_graph": wins_vs_subs_graph,
        "submission_graph": submission_graph,
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

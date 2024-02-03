import os

import plotly.express as px
from jinja2 import Environment, FileSystemLoader
import psycopg2

DB_URL = os.getenv("DB_URL")
if DB_URL is None:
    raise Exception("You must set the DB_URL environment variable in the lambda function settings")

connection = psycopg2.connect(
    DB_URL,
)
path = os.path.dirname(__file__)
env = Environment(loader=FileSystemLoader(path, encoding='utf8'))
print(path)

def handler(event, context):
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
    get_urls()

    data_canada = px.data.gapminder().query("country == 'Canada'")
    fig = px.bar(data_canada, x='year', y='pop')

    plotly_jinja_data = {"fig": fig.to_html(full_html=False)}
    # consider also defining the include_plotlyjs parameter to point to an external Plotly.js as described above

    template = env.get_template('wins_vs_finishes.html', )
    res = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "*/*"
        },
        "body": template.render(plotly_jinja_data)
    }

    return res


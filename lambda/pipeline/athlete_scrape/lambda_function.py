import dataclasses
import typing

import requests
import bs4


@dataclasses.dataclass
class Athlete:
    name: str
    nickname: str
    url: str


def handler(event, context):
    print("getting data from source")
    result = []

    def row_to_athlete_json(row: typing.List[bs4.element.Tag]):
        name = f"{data[0].text} {data[1].text}"
        name = name.replace("  ", " ")
        return dict(
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
            result.append(
                row_to_athlete_json(data)
            )
    return result

# bjjstats
WIP web app to allow users to visualize and explore bjj competitors records quickly and easily.

## Quickstart Pre-Reqs

 - clone the repository locally
 - in the `videobookmarks` directory, 
do`python3 -m pip install .` for setup

### Schema
![Alt text](img/schema.png)
`athlete` One entry per athlete

| field    | meaning                       |
|----------|-------------------------------|
| name     | Athletes full name. Required. |
| nickname | Optional.           |

`url` Lists the URLs that the data came from for that athlete

| field    | meaning                          |
|----------|----------------------------------|
| url      | The page used to scrape the data |

`performance` Each athlete has 1 performance for each match they participated in.

| field  | meaning                     |
|--------|-----------------------------|
| result | Win/Loss/Draw               |


`match` One entry per match, each match is linked to two performances, 
one performance from each athlete participating in the match

| field       | meaning                                                              |
|-------------|----------------------------------------------------------------------|
| year        | integer                                                              |
| competition | the name of the promotion (eg ADCC, IBJJF Worlds, IBJJF Euros, AIGA) |
| method      | how the match was won (eg. armbar, points (2-0), DQ)                 |
| stage       | the stage of the tournament eg quarterfinals, semifinals, finals     |
| weight      | the official weight class of the match                               |




### Making the lambda for athlete_scrape
The `athlete_scrape` folder under the `lambda` directory contains code for a scraper 
that is set up in AWS lambda to scrape new athletes and add them to the database.
I followed this guide to zip the athlete scrape function and upload it to Lambda:
https://medium.com/@jenniferjasperse/how-to-use-postgres-with-aws-lambda-and-python-44e9d9154513

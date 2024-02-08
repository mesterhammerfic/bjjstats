# bjjstats
WIP web app to allow users to visualize and explore bjj competitors records quickly and easily.

## Architecture
Batch ELT design:
![Alt text](img/bjjstats-system-design.png)
#### Key behavior of this design
Once a month, the entire warehouse is deleted and recreated using the 
latest data pulled from the website. When the data is pulled each month, 
it is stored in s3 before it is transformed to the format needed for 
the warehouse. The lambda functions are stored as Docker containers in ECS.

Benefits:
- **Simpler web scraping**: Since we're recreating the entire warehouse from scratch every time, 
we can simplify the scraper so that it pulls all the data every time without
needing to make a connection to the RDS data warehouse to determine what data
should be scraped.
- **Fault tolerance**: Storing the source data in s3 every month allows us to roll back to a working
state if the latest data broke the pipeline. All we need to do is restart the 
transformation lambda with an older version of the source data.
- **Modular**: Keeping each process on its own serverless lambda function allows
us to make tweaks or roll back each part of the process separately.

Drawbacks:
- The scraping process can be lengthy because I'm scraping every page
every time (about 5 min running an async scraper on my local machine).
- Storing all the source data in s3 requires maintenance to minimize storage
costs



Todo list:
- ~~write first web app endpoint with test data~~
- write extract & load lambda function and set up docker container
- write transformation lambda and set up docker container
- set up step function to automate the ELT pipeline
- set up eventbridge event to schedule regular data updates



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

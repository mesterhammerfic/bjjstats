# bjjstats
WIP web app to allow users to visualize and explore bjj competitors records quickly and easily.

## Architecture
Batch ETL design:
![Alt text](img/bjjstats-system-design.png)
#### Key behavior of this design
Once a month, the entire warehouse is deleted and recreated using the 
latest data pulled from the website. When the data is pulled each month, 
it is stored in s3 before it is loaded into the warehouse. The lambda 
functions are stored as Docker containers in ECS.

##### Why this design?
This design is chosen because it is simple and cost-effective. I could
choose to insert only the newest matches that were added to the source
data, but this would require complex logic to determine which matches
are already in the database.

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
This schema represents a many-to-many relationship between athletes 
and matches via the performance table.
![Alt text](img/schema.png)
`athlete` One entry per athlete

| field    | meaning                       |
|----------|-------------------------------|
| name     | Athletes full name. Required. |
| nickname | Optional.           |
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

# bjjstats
web app to allow users to visualize and explore bjj competitors records quickly and easily

## Quickstart Pre-Reqs

 - clone the repository locally
 - in the `videobookmarks` directory, 
do`python3 -m pip install .` for setup

### making the lambda for athlete_scrape
i had to zip this lambda function in order for psycopg to work
https://medium.com/@jenniferjasperse/how-to-use-postgres-with-aws-lambda-and-python-44e9d9154513

### lambda functions
I followed this guide to set up the lambda functions using docker images:
https://repost.aws/knowledge-center/lambda-container-images
Also, in the lambda function configuraton, 
you must set the environment variable DB_URL to the 
address of the postgres database you're using.


# A serverless ETL pipeline with AWS Lambda

- **Extract**: extracting information of [Youtube Trending videos](https://www.youtube.com/feed/trending) from [Youtube Search and Download API](https://rapidapi.com/h0p3rwe/api/youtube-search-and-download/). Those trending videos updates roughly every 15 minutes.
- **Transform**: cleaning and formatting data from the API’s JSON responses to Pandas Dataframes.
- **Load**: writing the data into AWS RDS MySQL instance.

## AWS Lambda 

Automate AWS Lambda function to run it every day to get updated data.

The Lambda function layers (Pandas, Numpy, Requests, SQLAlchemy, and PyMySQL) are from [Layers for Python 3.9](https://github.com/keithrozario/Klayers/tree/master/deployments/python3.9).

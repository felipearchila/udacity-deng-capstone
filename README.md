# Felipe Archila's Udacity Data Engineering Capstone Project
#### Summer/Autumn 2020

<a href="#scope">Scoping the Project and Gathering Data</a>

<a href="#explore">Exploring and Assessing the Data, Step by Step</a>

<a href="#model">The Data Model</a>

<a href="#runetl">Running the ETL</a>

<a href="#conclusion">In Conclusion</a>

<a href="#release">Release History</a>

<a href="#sources">Distinct Data Sources</a>

## Introduction

When I began my Data Engineering journey in June, I never imagined that by November, my instruction would allow me to see an entire Data Pipeline project through from scoping/sourcing, architecture, all the way to implementation and final analysis, which is where the *real* fun begins!

For my Capstone project, I have elected to analyze a dataset from an unlikely source... the City of New York. Through the Socrata API, the City of New York has published numerous datasets concerning all facets of municipal concern, including one subject area I was particularly interested in... **New York City Parking Violations**.

## <a name="scope">Scoping the Project and Gathering Data</a>

I discovered this dataset upon a search through that most famous of dataset repositories, [Kaggle](https://www.kaggle.com/), and it fascinated me because there is a treasure trove of insights that can be discerned from parking violation data.

The dataset itself is found here: [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)

My intention with this dataset is to create a dimensional-modeled cloud data warehouse using Amazon Redshift to provide the data for BI/analytics tools.

## <a name="explore">Exploring and Assessing the Data, Step by Step</a>

### Step One
My first step to explore the data was to create a new Jupyter Notebook on my local machine and just start playing with the Socrata library to load the data into a Pandas data frame.

```
import pandas as pd
from sodapy import Socrata

client = Socrata(domain="data.cityofnewyork.us")
results = client.get("pvqr-7yc4", limit=50000)
results_df = pd.DataFrame.from_records(results)
```

From there, I could do some simple analysis, such as what the column names are, what the distinct values of each column would be, and whether certain columns would qualify as potential dimensions. It was clear from the data that the grain was at the violation issuance level, meaning, one row per violation created at the time it was issued.

### Step Two
From my exploration, the `summons_number` column had emerged as a clear candidate for my key. It was unique for each record, and luckily, it is a `bigint` with no alphanumeric characters I had to wrestle with. This would make sorting and batching later a breeze.

The City of New York provides a Data Dictionary explaining all the different fields, and to some extent, there is standardization between the fields referenced and data that comes from the Department of Transportation, for example, when it comes to the state in which a vehicle is registered; however, whenever there is human input involved with certain fields, such as `violation_time` and `vehicle_color`, it is the Data Engineer's job to understand the subject area, business need, and/or the user's intent well enough to write some logic to clean up, standardize, or otherwise coalesce these values into something we can still JOIN on later.

In the case of `vehicle_color`, despite [New York State DMV's own Data Dictionary standardizing color codes](https://data.ny.gov/api/views/3pxy-wy2i/files/AUsdC2Y0iEymGyebFASIjDxZ7irrm1-_yS-o9qFzWTQ?download=true&filename=NYSDMV_VehicleSnowmobileAndBoat_Registration_Data%20Dictionary.pdf), it appears that each ticketing officer entered their own interpretation of what the vehicle's color is throughout the dataset. I ended up creating a UDF to run and add a standardized color code column in the Spark job that processes the full dataset to clean this up.

### Step Three
I then spun up an Amazon EMR cluster and ran a step to submit the job contained in `spark\process_violations.py` to make the Socrata API call in batches, process each batch by standardizing the `vehicle_color` input into one of a number of standard codes, added partitioning keys on Year and Month, and staged the processed data as JSON Lines in S3.

After enough exploration and a successful Spark job to process the full dataset, I was ready to decide on a conceptual data model based on the columns available and the use cases I'd been developing.

## <a name="model">The Data Model</a>

The model style I have chosen was the Star Schema because it allows for the greatest flexibility for open-ended analysis. The dataset is not intended to be updated in real-time, so more time can be spent in transforming and staging the data in a format that is logical and can be aggregated across a number of dimensions easily. Below is the list of tables and a brief description of each:

### Fact table:
- #### fact_parkingviolation
    - This is the full log of all parking violations issued in Fiscal Year 2021 in the City of New York. It contains foreign key columns to various dimensions, and the key measure for this Fact is the `fine_amount` assessed for the violation and, in some cases, the location of the violation.
    - Grain is at the violation issuance level
 
### Dimension tables:
- #### dim_borough
    - Contains information about the 5 Boroughs of New York City.
    - Data was sourced from borough names in the [Precinct dataset](https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page).
    - Grain is at the Borough level
- #### dim_date
    - Contains information about every distinct Date present in the parking violations dataset.
    - Grain is at the Date level
- #### dim_issuingagency
    - Contains information about the NYC Agencies that issue the violations.
    - Data was sourced from the column description of Issuing Agency on the [documentation page for the Parking Violations dataset](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)
    - Grain is at the Issuing Agency level
- #### dim_precinct
    - Contains information about the Police Precincts in which the violations occurred.
    - Data was sourced from [the NYPD Precinct homepage](https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page) and also provided Borough names for dim_borough
    - Grain is at the Precinct level
- #### dim_registrationstate
    - Contains information about the states or localities to which vehicles are registered.
    - Data was initially sourced from [Kaggle's USA Population dataset](https://www.kaggle.com/pompelmo/usa-states-geojson?select=usa_population_2019.csv) and I supplemented with [NYC Plate Types & State Codes dataset](http://www.nyc.gov/html/dof/html/pdf/faq/stars_codes.pdf)
    - Grain is at the Locality (State, Territory, Province, Country, or Government) level
- #### dim_time
    - Contains information about every distinct Time present in the parking violations dataset.
    - Grain is at the minute level
- #### dim_vehicle
    - Contains the cross product of all possible color codes, makes, and body styles of vehicle that could be issued a parking violation. Each record is assigned an integer surrogate key.
    - Data has multiple sources: the [Vehicle Makes and Body Types, Most Popular in New York State dataset](https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i) and [its corresponding Data Dictionary](https://data.ny.gov/api/views/3pxy-wy2i/files/AUsdC2Y0iEymGyebFASIjDxZ7irrm1-_yS-o9qFzWTQ?download=true&filename=NYSDMV_VehicleSnowmobileAndBoat_Registration_Data%20Dictionary.pdf)
    - Grain is at the vehicle make, body type, and color level
- #### dim_violation
    - Contains information about all the possible parking violation codes, their descriptions, and the fine they carry both at 96th Street and below in Manhattan and in the rest of the city.
    - Data is sourced from an [Attachment to the Parking Violations Dataset](https://data.cityofnewyork.us/api/views/pvqr-7yc4/files/7875fa68-3a29-4825-9dfb-63ef30576f9e?download=true&filename=ParkingViolationCodes_January2020.xlsx)
    - Grain is at the violation code level

## <a name="runetl">Running the ETL</a>
### Prerequisites
- Python 3
- All libraries listed in `pipeline\requirements.txt`
- An AWS Account and knowledge of your aws_access_key_id and aws_secret_access_key
- A running Amazon Redshift cluster

The first step is to clone, fork, or otherwise download my code, and do the following:
- Edit `pipeline\dwh.cfg` to add in your configuration values to connect to S3 and to your Amazon Redshift cluster
- Open a Terminal and run the following:
    - `python create_tables.py`
    - `python etl.py`

You will see progress messages along the way as you run each module. `etl.py` also includes data quality checks.

## <a name="conclusion">In Conclusion</a>

As I reflect on my time learning with Udacity and applying my knowledge to this Capstone project, the goal of making the Parking Violations dataset available for analysis is to provide a window into patterns and behavior of both the issuing agencies and the offending parkers in the City of New York to help formulate questions you didn't even know you had.

- Are more tickets issued during a certain time of day? Or month of the year?
- Are vehicles of a certain color or make more likely to receive a parking ticket?
- How are the parking fine revenues divided amongst the 5 boroughs of New York City?

Spark is actually already incorporated in the initial gathering of the data. I created a Spark job to run as a step in an Amazon EMR cluster to extract the full dataset from [data.cityofnewyork.us](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4) and perform pre-processing, such as my UDF to standardize the `vehicle_color` column, partition the data by year then month, and finally write the resulting JSON Lines data to my S3 bucket where I can then COPY the data to Redshift. This code can be found in `spark\process_violations.py`.

Airflow would be incorporated to run the pipeline in batches and load the data warehouse incrementally on a scheduled basis. I would leverage custom, templatable Operators to load all the Dimensions and the Fact table in a modular and easy-to-troubleshoot fashion.

I chose the Star Schema model because it allows for the greatest flexibility for users to come up with open-ended questions and analyze the data in ways I hadn't yet considered. I thought it important to leverage a columnar data warehouse for rapid querying and processing, and AWS was the best fit based on my experience throughout this course because I could seamlessly tie in the output of my Spark job in S3, stage the rest of the data in the same bucket, and have Redshift COPY and Transform the data into the model I wanted.

The data could be updated on an as-needed basis, but it's important to think about different scenarios:

- If the data was increased by 100x, the *initial* Spark job to stage the data would need to be scaled accordingly by adding a number of worker nodes. Although the current Spark job batches the API calls to 500,000 for each call, testing would be needed to balance how much higher that batch can go along with how the nodes are sized for memory. I'd double the memory on each node and increase the number of nodes by at least 10 to start. Subsequent Spark jobs to incrementally append the data would still need multiple nodes, but not as much as the initial load. The Redshift cluster would also need to be increased in size, both in number of nodes and compute available.
- If the pipelines were run on a daily basis by 7am, I'd look into partitioning the Spark output further by adding a Day grain after the Month partition, as well as introducing logic in the Spark job to only call the source API for `summons_numbers` greater than the highest existing number in the data warehouse to limit the size of data and make data available sooner. The COPY statement can then be amended to look only into the next available day rather than all of history as it currently does.
- If the database needed to be accessed by 100+ people, I would add more compute resources to ensure a quick experience for all. I would also research partitioning schemes and improve indexes and/or replication across my slices based on which queries are used the most and tune the environment accordingly.

## <a name="release">Release History</a>

|Release Number|Date|Remarks|
|---|---|---|
|1.00|November 9, 2020|First release! Spark code and Python pipeline included.|

## <a name="sources">Distinct Data Sources</a>
|Source Name|Hyperlink|Format|Remarks|
|---|---|---|---|
| NYC Finance \| Plate Types & State Codes|http://www.nyc.gov/html/dof/html/pdf/faq/stars_codes.pdf|CSV (manually generated)||
|Parking Violations Issued - Fiscal Year 2021|https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4|JSON/JSON Lines|Pre-processed using Spark job to extract data from [Socrata API](https://dev.socrata.com/) (source in `spark\process_violations.py`), transform, and stage in S3|
|Parking Violations Issued - Fiscal Year 2021 > ParkingViolationCodes_January2020.xlsx|https://data.cityofnewyork.us/api/views/pvqr-7yc4/files/7875fa68-3a29-4825-9dfb-63ef30576f9e?download=true&filename=ParkingViolationCodes_January2020.xlsx|CSV (manually generated)||
|Precincts - NYPD|https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page|CSV (manually generated)||
|USA states GeoJson \| Kaggle|https://www.kaggle.com/pompelmo/usa-states-geojson?select=usa_population_2019.csv|CSV||
|Vehicle Makes and Body Types, Most Popular in New York State|https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i|CSV (manually generated)||

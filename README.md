# Objective
This objective of this project is to work on big data in the AWS environment. Ingest dataset to Hdfs or S3 and save as Parquet format. Basic statistics, data analysis, visualization and machine learning using the ingested dataset.

# Dataset
We have ingested and completed analysis of New York City Yellow Taxi Data from 2019. Each individual trip record contains precise location coordinates for where the trip started and ended, timestamps for when the trip started and ended, plus a few other variables including fare amount, payment method, and distance traveled. The NYC Taxi and Limousine Commission (TLC), created in 1971, is the agency responsible for licensing and regualting New York City's medallion (yellow) taxis, street hail livery (green) taxis, for-hire vehicles (FHVs), commuter vans, and paratransit vehicles. The TLC collects trip record information for each taxi and for-hire vehicle trip completed by licensed drivers and vehicles. 

# Analysis
-	What were the top pick up locations by total charge and total trips?
- What was the average trip distance?
- What was the average trip time?
- What are the most popular months for pick ups?
- What are the most popular hours for pick ups?
- What is the average number  of pickups per month?
- What is the most popular payment type?
- What is the most popular ride type?

# Conclusions
- Each month there are 7,409,856 yellow cab rides in NYC (10,291 per hour!)
- The average distance of each ride was 2.96 miles
- The highest number of rides was in March, while the longest rides were in June
- Number of rides are lowest during 3am-5am, and highest during 5pm-7pm
- The average number of passengers is 1.57 people
- The most popular payment type was Credit Card
- The most popular rate code is standard fare
- Top 3 pick up locations by number of trip  are 237 (Upper East Side North), 161 (Midtown Center),    236 (Upper East Side North)
- Top 3 pick up locations by total fare 132 (JFK Airport), 138 (LaGuardia Airport), 161 (Midtown Center)


# Team Members
Chaerin Lee <br/>
Dean Duke <br/>
Ethan Goldbeck <br/>
Matthew Kuchar <br/>
Soumya Panda <br/>

# Technologies/Concepts Used
AWS S3 - To Ingest and Read Data <br/>
Python - PySpark <br/>
Data Analysis and Visualization <br/>
PySpark Machine Learning <br/>

# Dataset
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

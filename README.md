# Objective
This objective of this project is to work on big data in the AWS environment. Ingest dataset to Hdfs or S3 and save as Parquet format. Basic statistics, data analysis, visualization and machine learning using the ingested dataset.

# Dataset
We have ingested and completed analysis of New York City Yellow Taxi Data from 2019. Each individual trip record contains precise location coordinates for where the trip started and ended, timestamps for when the trip started and ended, plus a few other variables including fare amount, payment method, and distance traveled. The NYC Taxi and Limousine Commission (TLC), created in 1971, is the agency responsible for licensing and regualting New York City's medallion (yellow) taxis, street hail livery (green) taxis, for-hire vehicles (FHVs), commuter vans, and paratransit vehicles. The TLC collects trip record information for each taxi and for-hire vehicle trip completed by licensed drivers and vehicles. 

# Analysis
-	What were the Top pickup/dropoff locations? (by total charge and total trips)
-	What was the Average Trip distance? 
-	What was the Average Trip time?
-	What are the most popular months for pickups?
-	What are the most popular hours for pickups?

# Conclusions
-	The three pickup locations with the highest total charges are 138 161 230
-	The three pickup locations with the highest total trips are 138 161 230
-	The Average Trip Distance is 2.8 miles
-	The Average Trip Time is 17.72 minutes
-	1) March: 7834050 rides;    2) January: 7664883 rides;    3) May: 7563790 rides
-	7pm had the most rides on average, and 5am had the longest rides on average

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

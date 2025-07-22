# Data Analysis Project using Apache Hadoop & Apache Spark

This team project was developed for the course "Large Scale Data Management Systems" (2024), part of the NTUA MSc program in Data Science & Machine Learning. The goal was to work with large-scale datasets using modern data science tools and distributed computing technologies, specifically Apache Hadoop and Apache Spark.

The core objective was to gain hands-on experience in setting up, managing, and utilizing these distributed systems for large-scale data analysis tasks.

Each team consisted of two members collaborating to apply these technologies in practice.

For more information, please refer to the accompanying project_assignment.pdf file.

## Project Objectives

- **Gain hands-on experience** in setting up and managing distributed systems like Apache Hadoop and Apache Spark.
- **Apply Spark’s APIs** to process and analyze large-scale datasets.
- **Understand the capabilities and limitations** of these tools in the context of system resources and configuration choices.

## Primary Dataset

This dataset contains crime records for Los Angeles from 2010 to the present:

- [Crime Data from 2010 to 2019](https://data.lacity.org/A-Safe-City/Crime-Data-from-2010-to-2019/63jg-8b9z)
- [Crime Data from 2020 to Present](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/2nrs-mtv8)

## Secondary Datasets

- **LA Police Stations**  
  Contains the geographical locations of all 21 police stations in Los Angeles.  
   [LA Police Stations Dataset](https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore)

- **Median Household Income by ZIP Code (Los Angeles County)**  
  Provides income data by ZIP code based on census reports from 2015, 2017, 2019, and 2021. For this project, only the 2015 dataset was used.  
  [2015 Income Data](https://www.laalmanac.com/employment/em12c_2015.php)

- **Reverse Geocoding Dataset**  
  This dataset was provided by the instructors of the course and is used to map geographic coordinates (latitude and longitude) to ZIP codes within the Los Angeles area.  
  It contains the following columns:

  - `LAT`: Latitude  
  - `LON`: Longitude  
  - `ZIPcode`: Corresponding ZIP code


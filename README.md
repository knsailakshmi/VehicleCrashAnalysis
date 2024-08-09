# Car Crash Analysis

This project analyzes car crash data to provide insights into various aspects of traffic accidents. Using Apache Spark, the project performs data processing and analysis to answer critical questions related to crash statistics, vehicle makes, driver demographics, and more.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup and Installation](#setup-and-installation)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [Data Sources](#data-sources)

## Introduction

The Car Crash Analysis project leverages Apache Spark to process and analyze traffic accident data. The analysis includes various tasks such as identifying trends in crash statistics, examining the involvement of different vehicle makes, and assessing the impact of factors like alcohol on crash severity.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Java JDK 22**: Required for running Spark.
- **Apache Spark 3.5.1**: For distributed data processing.
- **Apache Maven 3.9.8**: For managing project dependencies.
- **Python 3.9.5**: Required for running the analysis scripts.
- **PySpark**: The Python API for Spark.

## Setup and Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/knsailakshmi/VehicleCrashAnalysis.git
   
2. **Navigate to Project Directory**
   Go to the Project Directory:
    ```bash
    $ cd VehicleCrashAnalysis
   
3. **Build and Setup**
     On the terminal, run the following command to create the `.egg` file:
   ```bash
   python setup.py bdist_egg

4.  **Execution**
    ```bash
    spark-submit --master "local[*]" --py-files dist/VehicleAccidentAnalysis-0.0.1-py3.9.egg main.py and get the output in Terminal as well as in Output Folder. [Edit your version of Python] 

## Configuration

The config.yaml file contains paths to input data files and output directories. Ensure it is properly configured before running the application.


## Usage

This application performs below analysis

1.	Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Data Sources

- **Data/**: Contains CSV files with the raw data:
  - `Charges_use.csv`
  - `Damages_use.csv`
  - `Endorse_use.csv`
  - `Restrict_use.csv`
  - `Primary_Person_use.csv`
  - `Units_use.csv`

- **Output/**: Folder where the results will be saved.

- **src/**
  - **Code/**
    - `utils.py`: Contains utility functions for loading and saving data.
    - `AccidentAnalysis.py`: Contains the `AccidentAnalysis` class with methods for different analyses.
    - `main.py`: The entry point for the Spark application.

- **setup.py**: Script to build the Python egg file.

- **config.yaml**: Configuration file with paths for input and output files.
##  Data Engineering Project Write-Up

**Abstract**

The goal of this project was to create a data storage and processing pipeline for an interactive dashboard to visualize summary information for Covid-19 vaccine adverse effects. The self-reported adverse event data is from VAERS, which was subsequently cleaned and processed to create table and graph components for the dashboard: details on the max number of cases reported, the demographics for reports per vaccine, and the most frequently reported symptoms.

**Design**

This dashboard is intended for the general public, just to glean some big-picture insights on adverse reactions and observe patterns. This is not to make health decisions, since the data is voluntarily provided and subject to bias. I hope it can show that even though it may seem like there are a lot of adverse events reported, when looking at the top adverse events reported for each vaccine, they are the side effects associated with many vaccines in general—headache, muscle soreness, fever, etc. The intention for this project is to make it easy and streamlined to keep the dashboard updated when new data is acquired.


**Data**

The data for the dashboard is from Vaccine Adverse Event Reporting System (VAERS), which is co-managed by the CDC and FDA, to detect possible safety problems in vaccines. I downloaded the CSV files from three years: 2020 (start of Covid-19 vaccination), 2021, and 2022. There are 3 CSV files for each year: one that contains all the events (with date, patient demographics, etc.), one that has the vaccine information (manufacturer, site, route of administration, etc.), and one with all the coded symptoms for a given event. These were stored as tables in SQLite. The full dataset consisted of about 860,000 reported events. As of the most recent data downloaded for this dashboard, 700,000 of the reports were for a Covid-19 vaccine.  

**Algorithms**

Preprocessing:
The three tables were joined by the VAERS_ID (common key), to then limit the data to Covid-19 vaccines. I focused on aggregating the data to create two smaller datasets: one for demographic information (average age and average length of time for onset of symptoms, grouped by vaccine manufacturer, dose number, and administration site), and the other for symptoms (most common symptoms grouped by vaccine manufacturer and dose number).

Visualization:
The dashboard displays the highest number of cases (when grouped by manufacturer, vaccine dose number, and administration location) and its associated details. The number of reports per vaccine can be visualized by dose number or by administration location (user input), through a stacked bar graph. At the bottom of the dashboard, the user can specify the manufacturer and dose number to see the top 10 symptoms reported along with some demographic information.

**Tools**

- Data storage and extraction: SQLite, SQLAlchemy
- Data processing: PySpark, pandas
- Deployment: Plotly Express, Dash, Heroku

**Communication**

The data and code are in this repo, along with the final slides. The dashboard was deployed to Heroku and can be accessed here: https://covid-vax-vaers.herokuapp.com/

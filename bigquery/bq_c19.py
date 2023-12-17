# From assignment:
# "Perform exploratory analysis on this dataset and produce a showcase/storyline of a few interesting patterns, visualizations, and your observations. One of these should be a timeline by month of COVID aggregate deaths for New York state. And another one can be anything of your choosing. You will walk us through your findings during our interview and use any tools you like."

# Get the auth credentials from IAM and Admin in Google Cloud
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-file.json"
# note: gcloud cli `gcloud auth login` did not work, but json auth did.
from google.cloud import bigquery

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns  # seaborn is a data viz library based on matplotlib
import requests
import json

# Set a nice plot style
sns.set_context("notebook")
sns.set_style("darkgrid")

# Create a "Client"
client = bigquery.Client(project="wellco-408202")

# Reference to the "us_states" dataset
dataset_ref = client.dataset("covid19_nyt", project="bigquery-public-data")

# Fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Reference to the "us_states" table
table_ref = dataset_ref.table("us_states")

# Fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "us_states" table
preview = client.list_rows(table, max_results=5).to_dataframe()
print("The table has {} rows and {} columns.".format(table.num_rows, len(table.schema)))
print(preview.head())

# Assertions to check that the table is correct
assert table.num_rows == 61942
assert len(table.schema) == 5  # i.e., five columns

# Define the SQL query that creates a column for daily confirmed cases and a column for daily deaths
query = """
WITH us_states_day AS (
  SELECT
      state_name,
      date,
      confirmed_cases,
      deaths,
      confirmed_cases - LAG(confirmed_cases) OVER (PARTITION BY state_name ORDER BY date) AS confirmed_cases_day,
      deaths - LAG(deaths) OVER (PARTITION BY state_name ORDER BY date) AS deaths_day
  FROM
     `bigquery-public-data.covid19_nyt.us_states`
)
SELECT
      date,
      state_name,
      confirmed_cases_day,
      deaths_day,
      (
         (confirmed_cases_day - AVG(confirmed_cases_day) OVER (PARTITION BY state_name))
         / NULLIF(STDDEV(confirmed_cases_day) OVER (PARTITION BY state_name), 0)
      ) AS confirmed_cases_day_zscore,
      (
         (deaths_day - AVG(deaths_day) OVER (PARTITION BY state_name))
         / NULLIF(STDDEV(deaths_day) OVER (PARTITION BY state_name), 0)
      ) AS deaths_day_zscore
  FROM
      us_states_day
  ORDER BY date;
"""

# Run the query and get the results as a Pandas DataFrame
data = client.query(query).to_dataframe()
# Convert 'date' column to datetime format
data["date"] = pd.to_datetime(data["date"])

# Check data types
# print(data.dtypes)
# date                           dbdate
# state_name                     object
# confirmed_cases_day             Int64
# deaths_day                      Int64
# confirmed_cases_day_zscore    float64
# deaths_day_zscore             float64

# Check for missing data
# print("Null values in dataset", data.isnull().sum())
# Null values in dataset
# date                           0
# state_name                     0
# confirmed_cases_day           56
# deaths_day                    56
# confirmed_cases_day_zscore    56
# deaths_day_zscore             56

# Fill null values with 0
# data = data.fillna(0)

# Assert there are no negative values in confirmed cases and deaths (not possible)
# assert data["confirmed_cases_day"].min() >= 0
# assert data["deaths_day"].min() >= 0

# Uh Oh, there are negative values in my calculated daily confirmed cases and deaths. This likely means there were corrections inputted to the data. I will fill these values with 0.
data["confirmed_cases_day"] = data["confirmed_cases_day"].clip(lower=0)
data["deaths_day"] = data["deaths_day"].clip(lower=0)

# Filter for New York, sort by date
ny_data = data[data["state_name"] == "New York"]
ny_data = ny_data.sort_values(by="date")

# MONTHLY AGGREGATE DEATHS IN NEW YORK
ny_data["month"] = ny_data["date"].dt.to_period("M")  # Create a month column
monthly_deaths = (
    ny_data.groupby("month")["deaths_day"].sum().reset_index()
)  # Sum deaths by month


## Plotting monthly deaths
plt.figure(figsize=(12, 6))
ax = sns.barplot(
    x=monthly_deaths["month"].astype(str), 
    y=monthly_deaths["deaths_day"], 
    color="#E74C3C"
)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
ax.set_xlabel("Month", fontsize=14)
ax.set_ylabel("Aggregate Deaths", fontsize=14)
ax.set_title("Monthly Aggregate Deaths in New York", fontsize=16)

# Format y-axis labels to avoid scientific notation
ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

plt.tight_layout()
plt.show()

# Plotting Z-score over time (daily confirmed cases and deaths)
plt.figure(figsize=(12, 6))
plt.plot(
    ny_data["date"],
    ny_data["confirmed_cases_day_zscore"],
    label="Confirmed Cases Day Z-Score",
    color="#2980B9",
    linewidth=2,
)
plt.plot(
    ny_data["date"],
    ny_data["deaths_day_zscore"],
    label="Deaths Day Z-Score",
    color="#E74C3C",
    linewidth=2,
)
plt.xticks(rotation=45, ha="right")
plt.xlabel("Date", fontsize=12)
plt.ylabel("Confirmed Cases Day Z-Score", fontsize=12)
plt.title("Deaths and Cases Z-Score for New York", fontsize=16)
plt.legend()
plt.tight_layout()
plt.show()

# Anomaly Detection using IQR
# Find and label Z-scores that are 3 standard deviations away from the mean
upper_bound = 3
lower_bound = -3

# Find outliers
outliers = ny_data[
    (ny_data["confirmed_cases_day_zscore"] > upper_bound)
    | (ny_data["confirmed_cases_day_zscore"] < lower_bound)
]
outliers_deaths = ny_data[
    (ny_data["deaths_day_zscore"] > upper_bound)
    | (ny_data["deaths_day_zscore"] < lower_bound)
]

# Plotting anomalies on top of Z-score plot
plt.figure(figsize=(12, 6))
plt.plot(
    ny_data["date"],
    ny_data["confirmed_cases_day_zscore"],
    label="Confirmed Cases Day Z-Score",
    color="#2980B9",
    linewidth=2,
)
plt.plot(
    ny_data["date"],
    ny_data["deaths_day_zscore"],
    label="Deaths Day Z-Score",
    color="#E74C3C",
    linewidth=2,
)
plt.scatter(
    outliers["date"],
    outliers["confirmed_cases_day_zscore"],
    color="red",
    label="Anomalies Cases",
)
plt.scatter(
    outliers_deaths["date"],
    outliers_deaths["deaths_day_zscore"],
    color="black",
    label="Anomalies Deaths",
)
plt.title("Confirmed Cases and Deaths w/ Anomalies (Z +- 3) for New York", fontsize=16)
plt.legend()
plt.tight_layout()
plt.show()

# Outlier Narrative
# What could be the reasons for those large outliers? Could be errors in reporting.
# Sort by highest or lowest z-scores for confirmed cases and deaths to see what days they are
print(ny_data.sort_values(by="confirmed_cases_day_zscore", ascending=False).head())
print(ny_data.sort_values(by="confirmed_cases_day_zscore", ascending=True).head())
print(ny_data.sort_values(by="deaths_day_zscore", ascending=False).head())
print(ny_data.sort_values(by="deaths_day_zscore", ascending=True).head())

# The day 2022-11-11 in NY had a high spike. Why?

# Option: Remove outliers from data and replot?

######## PART 1.c. ########

# Do this query, pull out data, and then find the maximum month for each state and plot it.

sql_query = """
SELECT
    state_name,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    MAX(confirmed_cases) OVER (
        PARTITION BY state_name, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
    ) AS highest_monthly_confirmed_cases
FROM 
    `bigquery-public-data.covid19_nyt.us_states`;
"""

data2 = client.query(sql_query).to_dataframe()
data2["date"] = pd.to_datetime(data2["date"])

print(data2.head())

#   state_name       date  year  month  highest_monthly_confirmed_cases
# 0    Alabama 2022-08-01  2022      8                          1491815
# 1    Alabama 2022-08-02  2022      8                          1491815
# 2    Alabama 2022-08-03  2022      8                          1491815
# 3    Alabama 2022-08-04  2022      8                          1491815
# 4    Alabama 2022-08-05  2022      8                          1491815

# It has data like this, but i need to group it by each state

# Get a unique row for each state
data2 = data2.groupby(["state_name", "year", "month"]).max().reset_index()

# Sort by highest monthly confirmed cases
data2 = data2.sort_values(by='highest_monthly_confirmed_cases', ascending=False)

states_and_highest_month = {}
for state in data2["state_name"].unique():
    state_data = data2[data2["state_name"] == state]
    highest_month = state_data["highest_monthly_confirmed_cases"].max()
    states_and_highest_month[state] = highest_month

print(states_and_highest_month)

# plot it w/ state on X, using SNS barplot, w/ a cool palette, comma sep Y values not scientific notation
# Plot the highest monthly confirmed cases by state

# Convert values to a numpy array for colormap normalization
values = np.array(list(states_and_highest_month.values()))
# Normalize values for the colormap
normalized_values = (values - values.min()) / (values.max() - values.min())

# Plot the highest monthly confirmed cases by state
plt.figure(figsize=(15, 8))
ax = sns.barplot(
    x=list(states_and_highest_month.keys()), 
    y=list(states_and_highest_month.values()), 
    palette=sns.color_palette("rocket", as_cmap=True)(normalized_values)
)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
ax.set_xlabel("State", fontsize=14)
ax.set_ylabel("Highest Monthly Confirmed Cases", fontsize=14)
ax.set_title("Highest Monthly Confirmed Cases by State", fontsize=16)

# Format y-axis labels to avoid scientific notation
ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

plt.tight_layout()
plt.show()

# NOTE: this should be divided by population to get a better idea of the spread of the virus

# Get population data from an api call

pop_per_state = requests.get(
    "https://datausa.io/api/data?drilldowns=State&measures=Population"
)
pop_per_state = json.loads(pop_per_state.text)
pop_per_state = pd.DataFrame(pop_per_state["data"])
pop_per_state = pop_per_state[["State", "Population"]]
pop_per_state.columns = ["state_name", "population"]
pop_per_state["population"] = pop_per_state["population"].astype(int)

# Merge population data with highest monthly confirmed cases data, like a table join.
data2 = data2.merge(pop_per_state, how="left", on="state_name")
data2["highest_monthly_confirmed_cases_per_capita"] = (
    data2["highest_monthly_confirmed_cases"] / data2["population"]
)

# Sort it by that
data2 = data2.sort_values(by='highest_monthly_confirmed_cases_per_capita', ascending=False)

# Plot the highest monthly confirmed cases by state per capita
plt.figure(figsize=(15, 8))
ax = sns.barplot(
    x=data2["state_name"], 
    y=data2["highest_monthly_confirmed_cases_per_capita"], 
    palette="viridis", 
    errwidth=0 # Remove error bars
)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
ax.set_xlabel("State", fontsize=14)
ax.set_ylabel("Highest Monthly Confirmed Cases per Capita", fontsize=14)
ax.set_title("Highest Monthly Confirmed Cases by State per Capita", fontsize=16)

# Format y-axis labels to avoid scientific notation
# ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

plt.tight_layout()
plt.show()

  
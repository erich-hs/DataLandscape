[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/1lXY_Wlg)

# MAD Dashboard
### `M`achine Learning, `A`nalytics, and `D`ata `Dashboard`


Inspired by [Matt Turck](https://www.linkedin.com/in/turck/)'s [MAD Landscape](https://mattturck.com/mad2023/) initiative to help data practitioners navigate and make sense of the rapidly evolving world of data products and frameworks, the **MAD Dashboard** aims at giving a birds-eye-view of the scene with insights about popularity and adoption of these tools.

With the project, my goal is to help answering questions such as:
> - What is the most popular Open Source OLAP Database?
> - Which NoSQL Database is experiencing the fastest adoption rate?
> - Which file formats are increasing in adoption? Which are decreasing?
> - What was the most popular MLOps framework 10 years ago? What is the most popular today?

And hopefully much more.

# Conceptual Data Modeling
The core reference dataset for this dashboard is sourced from the [2023 MAD Landscape]((https://mattturck.com/mad2023/)) by Matt Turck. Matt's analysis for 2023, however, is comprised of more than 1,400 logos and for a couple of reasons that list need to be trimmed down to a more manageable number of products/frameworks to be tracked.

A curated subset will, therefore, be manually selected from his [interactive dashboard](https://mad.firstmark.com/). This initial selection will be partially arbitrary, but the idea is to implement a scalable and extensible solution, where new products can be onboarded in the future.

To compose the metrics and datasets for analytics, data will be collected from data sources from three major categories:

- Social Media and Forums
- Download Metrics
- GitHub Metrics

![Conceptual Data Modeling](docs/images/Conceptual_Data_Modelling.png)

A short description of each dataset and an explanation of the color scheme is included below.

## Data Sources
### Social Media and Forums
This section will track daily (and possibly near-real-time) mentions of the selected data tools and frameworks.

**[Reddit API](https://www.reddit.com/dev/api/)**: Will be used to track daily mentions at the most relevant subreddits in the data community:
  - `r/dataengineering` (183k members)
  - `r/machinelearning` (2.9M members)
  - `r/datascience` (1.6M members)
  - `r/analytics` (163k members)
  - `r/localllama` (162k members)

The reddit API has gone through substantial policies changes in 2023 and the endpoints no longer reach the complete history of the posts in the platform. For this reason, historical data for an initial load and backfills up to December 2022 will be retrieved from [The Reddit Community Archive](https://the-eye.eu/redarcs/) by Pushshift.io and The-Eye.eu.

**[StackExchange API](https://api.stackexchange.com/docs)**: Will be used to track daily questions posted on StackOverflow. A public [BigQuery dataset](https://bigquery.cloud.google.com/dataset/bigquery-public-data:stackoverflow) is available for historical data up until September 2022.

**[Hacker News API](https://github.com/HackerNews/API)**: Will be used to track daily mentions at the Y Combinator Hacker News forum.

**[X(Twitter) API v2](https://developer.x.com/en/docs/twitter-api)**: The X(Twitter) API is marked red as its Free and Basic tiers have very limited rates to fetch Twitter posts. The nature of the platform also makes it difficult to openly track mentions of products and tools without a brute force search through all posts daily.

### Download Metrics
This section will track daily download metrics of the selected data tools and frameworks. The main caveat from this section is the lack of coverage of proprietary tools that require dedicated installers.

**[pypi metrics](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbigquery-public-data!2spypi)**: Public dataset of the Python Package Index downloads metrics. This is dataset contains download events of PyPI packages and currently spans 917 billion rows with 323TB.

**[npm registry](https://github.com/npm/registry/blob/main/docs/download-counts.md#per-version-download-counts)**: The npm registry is a publicly available dataset of daily snapshots of npm's log data.

**[Homebrew Formulae](https://formulae.brew.sh/analytics/)**: Homebrew's public available analytics data of install events. This dataset is orange because the data is publicly available only via an aggregated count of events for the past 30, 90, and 365 days.

### GitHub Metrics
**[GitHub API](https://docs.github.com/en/rest/quickstart)**: The GitHub APIs will be used to track numbers from the repositories associated with the list of products and frameworks listed in the dashboard. This might include:
- Star count
- Contributors count
- Open issues
- Open discussions

## Metrics and Data Sources


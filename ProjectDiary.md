# Project Diary

The aim of this file is to describe how the development process went: issues, challenges and clever solutions.

## Phase 1: Historical Data

### 1.1: Downloading Data from GH Archive

We wrote a Python downloader to get data from GH Archive in `.json.gz` format and then convert it to `.parquet`, month by month in a given period. The code was optimized to run locally because all the necessary files to create the Parquet archive for a given month were deleted as soon as they had been used.

Challenges:

- The first implementation downloaded files sequentially: this took a very long amount of time. Thus, the code was edited to handle downloads in parallel. This drastically reduced the time needed to run the script.

## Phase 3.1 - Dockerization

We then proceeded to create the Docker ecosystem in order to effectively deploy the project

# Chi Hack Night Ghost Buses Breakout Group

This is the repo for the [Chi Hack Night Ghost Buses breakout group](https://github.com/chihacknight/breakout-groups/issues/217)! **As of September 2024, the [Mansueto Institute for Urban Innovation](https://miurban.uchicago.edu/) is taking over maintenance of the data and data pipelines originally created for this project. Archived real-time bus location data dating back to May 2022 will remain publicly available through them. This repo is no longer actively maintained. It is preserved in its state as of September 2024 for reference.**

## AWS architecture
__Updated May 19, 2022__

Currently, the AWS setup is based on [this post](https://towardsdatascience.com/serverless-covid-19-data-scraper-with-python-and-aws-lambda-d6789a551b78) with some references to the [CPS Covid Dashboard back-end](https://github.com/misterjacko/CPS-COVID-BE).

TODO: Describe how to make updates to the code in AWS

## Getting started
Set up a virtual environment with `python3 -m venv .venv` and activate it with `source .venv/bin/activate` (`.venv\Scripts\activate` in Windows). Then run `pip install -r requirements.txt` to install the required packages.

To set environment variables, create a `.env` file in the top-level directory and add the following contents:
```
BUCKET_PUBLIC='chn-ghost-buses-public'
BUCKET_PRIVATE='chn-ghost-buses-private'
```
Note that there may be permissions issues when running `rt_daily_aggregations.py` with `BUCKET_PUBLIC` as the `BUCKET_NAME`.

## Pre-commit hooks
Pre-commit hooks are defined in `.pre-commit-config.yaml`, based on this [post](https://towardsdatascience.com/pre-commit-hooks-you-must-know-ff247f5feb7e). The hooks will enforce style with the linter `black`, check for commited credentials and extraneous debugger breakpoints, and sort imports, among other things.

To get the latest versions of dependencies, update the `.pre-commit-config.yaml` file with `pre-commit autoupdate`. To run the hooks against all files without committing, use `pre-commit run --all-files`. To have the pre-commit hooks run when executing `git commit`, use `pre-commit install`.

# BQ Jobrunner

## About
This library enables users to easily query multiple BigQuery queries. It takes dependencies between tables in consideration so that you can run multiple dependent queries at once.

You do not need to worry about anything such as a table resulting from a large sized query like you would have to set on the Google BigQuery's query settings. Everything you need is a path to a bq file and a main python code which includes this library.

For more information, please refer to the sample file in this repository; otherwise, contact us.


## Getting Started
Run the following to start developing.
```shell
pip install -r requirements.txt
```

## For Testing
Run below before testing the changes you have made.
```python
python setup.py sdist & python setup.py bdist_wheel

pip install .
```

## Contributors
Takuto Sugisaki ([@tsintermax](https://github.com/tsintermax))

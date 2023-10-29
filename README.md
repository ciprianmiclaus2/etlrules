# etlrules


<p align="center">
<a href="https://pypi.python.org/pypi/etlrules">
    <img src="https://img.shields.io/pypi/v/etlrules.svg"
        alt = "Release Status">
</a>

<a href="https://github.com/ciprianmiclaus/etlrules/actions">
    <img src="https://github.com/ciprianmiclaus/etlrules/actions/workflows/python-package.yml/badge.svg?branch=main" alt="CI Status">
</a>

<a href="https://codecov.io/gh/ciprianmiclaus/etlrules" > 
 <img src="https://codecov.io/gh/ciprianmiclaus/etlrules/graph/badge.svg?token=4N0N8XSVZY"/> 
 </a>

<a href="https://ciprianmiclaus.github.io/etlrules/">
    <img src="https://img.shields.io/website/https/ciprianmiclaus.github.io/etlrules/index.html.svg?label=docs&down_message=unavailable&up_message=available" alt="Documentation Status">
</a>

</p>


A python rule engine for applying transformations to dataframes.

ETL stands for [Extract, Trasform, Load](https://en.wikipedia.org/wiki/Extract,_transform,_load), which is a three step
process to source the data from some data source (Extract), transform the data (Transform) and publish it to a final
destination (Load).

Data transformation of tabular sets can be done in pure python with many dedicated python packages, the most widely
recognized being [pandas](https://pandas.pydata.org/). The result of such transformations can be quite opaque with the
logic difficult to read and understand, especially by non-coders. Even coders can struggle to understand certain
transformations unless in-code documentation is added and even when documentation is available, the code change in ways
which renders the documentation stale.

The etlrules package solves this by offering a set of simple rules which users can use to form a plan. The plan is a blueprint
on how to transform the data. The plan can be saved to a yaml file, stored in a repo for version control or in a database for
manipulation via UIs and then executed in a repeatable and predictable fashion. The rules in the plan can have names and
extensive description acting as an embedded documentation as to what the rule is trying to achieve.

This data-driven way of operating on tabular data allows non-technical users to come up with data transformations which can
automate various problems without the need to code. Workflows for managing change and version control can be put in place
around the plans, allowing technical and non-technical users to collaborate on data transformations that can be scheduled to
run periodically for solving real business problems.

## High level concepts

### Plan

A plan is a blueprint of how to perform extractions of tabular data, transformations of the data and how to load (ie write)
the transformed data to its final destination.

A plan is a collection of rules, each of which operate on a dataframe (tabular data).

### Rule

A rule is an operation performed on a dataframe. There are three types of rules:
    * Extract rules (aka read rules)
        - They will read an external data source (ie files, DBs, APIs endpoints) and bring the data into memory for processing
    * Transform rules
        - They will perform a transformation of the data (ie add a new column, modify an existing column, join columns, aggregate)
    * Load rules (aka write rules)
        - They will write the output into an external storage (ie files, DBs, APIs endpoints)

### Rule engine

The component that takes a plan and executes it (rule by rule) based on an input.


### Rule data

The structure that holds together any input dataframes, temporary results and the final output of a rule engine execution of a plan.
The rule data can have some input dataframes or they can start as empty canvases, with the plan performing extractions/reading of data that it needs.

### Backend

The underlying dataframe library to use for executing the plan. For example: pandas, vaex, polars, etc.
At the moment, only pandas is supported.


## Documentation

<https://ciprianmiclaus.github.io/etlrules/>


## License

Free software: MIT

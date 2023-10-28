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

## Documentation

<https://ciprianmiclaus.github.io/etlrules/>


## License

Free software: MIT
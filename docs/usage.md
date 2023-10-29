# Usage

etlrules allows you to extract tabular data (ie a dataframe) from a data source,
transform it through a set of rules and load the result into another data source.

## Basic pipeline mode example

Let's look at an example that runs a basic plan in a pipeline mode.
A pipeline mode is a simple mode where each rule is executed in the same order they are
added to the plan, with each rule taking its input from the rule before it and passing the
output to the rule following it.

The example below also prepares the input dataframe directly into a RuleData instance.
Other plans can extract their own data from files, DBs, API endpoints, etc. with the details
on how to extract the data in the plan itself.
Similarly, the plan below doesn't load the result anywhere, the result being available in the
RuleData to be inspected at the end. More complex plans can load (ie write) the result to 
files, DBs, API endpoints with all the details encapsulated in the plan.

### Step 1 - Create a plan

The plan is a blueprint for data extractions, data transformations and data loading.
Below, we're going to create a plan with 3 rules:

``` console

from etlrules import Plan
from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule

plan = Plan()
plan.add_rule(SortRule(['A']))
plan.add_rule(ProjectRule(['A', 'B']))
plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}))

```

The plan above will:
    1. Sort the input dataframe by column A
    2. Project columns A and B (ie only keep columns A and B)
    3. Rename column A to AA and column B to BB

### Step 2 - Prepare an input dataframe

We are going to create an input pandas dataframe and wrap it into a RuleData instance.
RuleData is what the rules engine operates with. RuleData will hold the inputs, outputs
and any intermediate results/dataframes.

We're going to work with the following input dataframe:

| A  | B  | C     |
|----|----|-------|
| 2  | n  | True  |
| 1  | m  | False |
| 3  | p  | True  |


``` console

from pandas import DataFrame
from etlrules import RuleData

input_df = DataFrame(data=[
    {'A': 2, 'B': 'n', 'C': True},
    {'A': 1, 'B': 'm', 'C': False},
    {'A': 3, 'B': 'p', 'C': True},
])
data = RuleData(input_df)

```

### Step 3 - Run the plan

Running the plan will apply the rules in turn over the dataframe.
We will need to instantiate a RuleEngine and run the plan.

``` console

from etlrules import RuleEngine

rule_engine = RuleEngine(plan)
rule_engine.run(data)

```

### Step 4 - Inspect the result

The RuleData will contain the result.

``` console

result = data.get_main_output()
print(result)

```

The example should produce the following transformed dataframe:

| AA  | BB  |
|-----|-----|
| 2   | n   |
| 1   | m   |
| 3   | p   |

We've just completed our first etlrules application.
We can also serialize our plan to yaml, save it to a file, a repo for version control or a database.
We can add names and extensive descriptions to all the rules as a form of documentation for the plan.


## Basic graph mode example

### Step 1 - Create a plan

We'll use the same plan from the pipeline example; the only difference being the named inputs/outputs are used.

``` console

from etlrules import Plan
from etlrules.backends.pandas import ProjectRule, RenameRule, SortRule

plan = Plan()
plan.add_rule(SortRule(['A'], named_input="input", named_output="sorted_data"))
plan.add_rule(ProjectRule(['A', 'B']), named_input="sorted_data", named_output="projected_data")
plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}), named_input="projected_data", named_output="result")

```

Note::
    Because the rules specify what inputs they need and what outputs they produce, the order they are
    added to the plan is irrelevant. They will be executed in the order of dependency.

As such, we can also add the rules to the plan in a different order, e.g.:

``` console

plan.add_rule(RenameRule({'A': 'AA', 'B': 'BB'}), named_input="projected_data", named_output="result")
plan.add_rule(SortRule(['A'], named_input="input", named_output="sorted_data"))
plan.add_rule(ProjectRule(['A', 'B']), named_input="sorted_data", named_output="projected_data")

```

### Step 2 - Prepare an input dataframe

In the plan above, the single input is a named input called "input".
We will use the same dataframe as in the pipeline example, but name it as "input" in the RuleData.

``` console

from pandas import DataFrame
from etlrules import RuleData

input_df = DataFrame(data=[
    {'A': 2, 'B': 'n', 'C': True},
    {'A': 1, 'B': 'm', 'C': False},
    {'A': 3, 'B': 'p', 'C': True},
])
data = RuleData(named_inputs={"input": input_df})

```

### Step 3 - Run the plan

Running the plan is no different in graph mode to the pipeline mode.

``` console

from etlrules import RuleEngine

rule_engine = RuleEngine(plan)
rule_engine.run(data)

```

### Step 4 - Inspect the result

The RuleData will contain the result, named as "result" since the last rule, the RenameRule
produces a named output called "result".

``` console

result = data.get_named_output("result")
print(result)

```

The example should produce the following transformed dataframe:

| AA  | BB  |
|-----|-----|
| 2   | n   |
| 1   | m   |
| 3   | p   |

The other intermediary dataframe can also be inspected.

``` console

sorted_data = data.get_named_output("sorted_data")
print(sorted_data)

```

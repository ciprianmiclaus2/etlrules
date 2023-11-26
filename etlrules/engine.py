import graphlib
from typing import Optional, Tuple, Union

from .data import RuleData, context
from .exceptions import GraphRuntimeError, InvalidPlanError
from .plan import PlanMode, Plan


class RuleEngine:
    """ Run a set of extract/transform/load rules over a dataframe.

    Takes in a plan with the definition of the extract/transform/load rules and it
    runs it over a RuleData instance. The RuleData instance can be optionally pre-populated with
    a input dataframe (in pipeline mode) or a sequence of named inputs (named dataframes).
    
    The plan can have rules to extract data (ie add more dataframes to the RuleData). It can have
    transform rules which will transform the existing dataframes (either in-place or produce new
    named dataframes). It can also have rules to load data into external systems, e.g. files,
    databases, API connections, etc.

    At the end of a plan run, the RuleData instance passed in will contain the results of the run
    (ie new dataframes/transformed dataframes) which can be inspected/operated on outside of the
    rule engine.
    """

    def __init__(self, plan: Plan):
        assert isinstance(plan, Plan)
        self.plan = plan

    def _get_context(self, data: RuleData) -> dict[str, Union[str, int, float, bool]]:
        context = {}
        context.update(self.plan.get_context())
        context.update(data.get_context())
        return context

    def run_pipeline(self, data: RuleData) -> RuleData:
        with context.set(self._get_context(data)):
            for rule in self.plan:
                rule.apply(data)
        return data

    def _get_topological_sorter(self, data: RuleData) -> graphlib.TopologicalSorter:
        g = graphlib.TopologicalSorter()
        existing_named_outputs = set(name for name, _ in data.get_named_outputs())
        named_outputs = {}
        for idx, rule in enumerate(self.plan):
            if rule.has_output():
                named_outputs_lst = list(rule.get_all_named_outputs())
                if not named_outputs_lst:
                    raise InvalidPlanError(f"Rule {rule.__class__}/(name={rule.get_name()}, index={idx}) has no named outputs.")
                for named_output in named_outputs_lst:
                    if named_output is None:
                        raise InvalidPlanError(f"Rule {rule.__class__}/(name={rule.get_name()}, index={idx}) has empty named output.")
                    existing_rule = named_outputs.get(named_output)
                    if existing_rule is not None:  
                        raise InvalidPlanError(f"Named output '{named_output}' is produced by multiple rules: {rule.__class__}/(name={rule.get_name()}) and {existing_rule[1].__class__}/(name={existing_rule[1].get_name()})")
                    named_outputs[named_output] = (idx, rule)
        named_output_clashes = existing_named_outputs & set(named_outputs.keys())
        if named_output_clashes:
            raise GraphRuntimeError(f"Named output clashes. The following named outputs are produced by rules in the plan but they also exist in the input data, leading to ambiguity: {named_output_clashes}")
        for idx, rule in enumerate(self.plan):
            if rule.has_input():
                named_inputs = list(rule.get_all_named_inputs())
                if not named_inputs:
                    raise InvalidPlanError(f"Rule {rule.__class__}/(name={rule.get_name()}, index={idx}) has no named inputs.")
                for named_input in named_inputs:
                    if named_input is None:
                        raise InvalidPlanError(f"Rule {rule.__class__}/(name={rule.get_name()}, index={idx}) has empty named input.")
                    if named_input in named_outputs:
                        g.add(idx, named_outputs[named_input][0])
                    elif named_input not in existing_named_outputs:
                        raise GraphRuntimeError(f"Rule {rule.__class__}/(name={rule.get_name()}, index={idx}) requires a named_input={named_input} which doesn't exist in the input data and it's not produced as a named output by any of the rules in the graph.")
                    else:
                        g.add(idx)
            else:
                g.add(idx)
        return g

    def run_graph(self, data: RuleData) -> RuleData:
        g = self._get_topological_sorter(data)
        g.prepare()
        with context.set(self._get_context(data)):
            while g.is_active():
                for rule_idx in g.get_ready():
                    rule = self.plan.get_rule(rule_idx)
                    rule.apply(data)
                    g.done(rule_idx)
        return data

    def validate_pipeline(self, data: RuleData) -> Tuple[bool, Optional[str]]:
        return True, None

    def validate_graph(self, data: RuleData) -> Tuple[bool, Optional[str]]:
        try:
            self._get_topological_sorter(data)
        except (InvalidPlanError, GraphRuntimeError) as exc:
            return False, str(exc)
        return True, None

    def validate(self, data: RuleData) -> Tuple[bool, Optional[str]]:
        assert isinstance(data, RuleData)
        if self.plan.is_empty():
            return False, "An empty plan cannot be run."
        mode = self.plan.get_mode()
        if mode == PlanMode.PIPELINE:
            return self.validate_pipeline(data)
        elif mode == PlanMode.GRAPH:
            return self.validate_graph(data)
        return False, "Plan's mode cannot be determined."

    def run(self, data: RuleData) -> RuleData:
        assert isinstance(data, RuleData)
        if self.plan.is_empty():
            raise InvalidPlanError("An empty plan cannot be run.")
        mode = self.plan.get_mode()
        if mode == PlanMode.PIPELINE:
            return self.run_pipeline(data)
        elif mode == PlanMode.GRAPH:
            return self.run_graph(data)
        else:
            raise InvalidPlanError("Plan's mode cannot be determined.")

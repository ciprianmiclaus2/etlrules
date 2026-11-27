""" The pyarrow backend: the rules operate on pyarrow tables (pyarrow.Table). """
from .aggregate import AggregateRule
from etlrules.backends.common.basic import ProjectRule
from .basic import DedupeRule, ExplodeValuesRule, LimitRule, RenameRule, ReplaceRule, SortRule
from .concat import VConcatRule, HConcatRule
from .conditions import IfThenElseRule, FilterRule, FilterValuesRule
from .datetime import (
    DateTimeLocalNowRule, DateTimeUTCNowRule, DateTimeToStrFormatRule,
    DateTimeRoundRule, DateTimeRoundDownRule, DateTimeRoundUpRule,
    DateTimeExtractComponentRule, DateTimeAddRule, DateTimeSubstractRule,
    DateTimeDiffRule, StrToDateTimeRule,
)
from .fill import ForwardFillRule, BackFillRule
from .nulls import FillNullRule, DropNullRowsRule, CoalesceRule
from .joins import LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule, SemiJoinRule, AntiJoinRule
from .validation import AssertNotNullRule, AssertUniqueRule
from .newcolumns import AddNewColumnRule, AddRowNumbersRule
from .numeric import AbsRule, ClipRule, RoundRule
from .strings import (
    StrLowerRule, StrUpperRule, StrCapitalizeRule, StrSplitRule, StrSplitRejoinRule,
    StrStripRule, StrPadRule, StrExtractRule, StrSliceRule, StrLengthRule, StrContainsRule,
)
from .types import TypeConversionRule
from etlrules.backends.common.basic import RulesBlock

## IO - extractors and loaders
from .io.files import ReadCSVFileRule, ReadParquetFileRule, WriteCSVFileRule, WriteParquetFileRule, ReadJSONFileRule, WriteJSONFileRule
from .io.db import ReadSQLQueryRule, WriteSQLTableRule


__all__ = [
    'AggregateRule',
    'DedupeRule', 'ExplodeValuesRule', 'ProjectRule', 'RenameRule', 'ReplaceRule', 'SortRule', 'LimitRule',
    'VConcatRule', 'HConcatRule',
    'IfThenElseRule', 'FilterRule', 'FilterValuesRule',
    'DateTimeLocalNowRule', 'DateTimeUTCNowRule', 'DateTimeToStrFormatRule',
    'DateTimeRoundRule', 'DateTimeRoundDownRule', 'DateTimeRoundUpRule',
    'DateTimeExtractComponentRule', 'DateTimeAddRule', 'DateTimeSubstractRule',
    'DateTimeDiffRule', 'StrToDateTimeRule',
    'ForwardFillRule', 'BackFillRule',
    'FillNullRule', 'DropNullRowsRule', 'CoalesceRule',
    'LeftJoinRule', 'InnerJoinRule', 'OuterJoinRule', 'RightJoinRule', 'SemiJoinRule', 'AntiJoinRule',
    'AssertNotNullRule', 'AssertUniqueRule',
    'AddNewColumnRule', 'AddRowNumbersRule',
    'AbsRule', 'RoundRule', 'ClipRule',
    'StrLowerRule', 'StrUpperRule', 'StrCapitalizeRule', 'StrSplitRule', 'StrSplitRejoinRule',
    'StrStripRule', 'StrPadRule', 'StrExtractRule', 'StrSliceRule', 'StrLengthRule', 'StrContainsRule',
    'TypeConversionRule',
    'RulesBlock',
    # IO extractors and loaders
    'ReadCSVFileRule', 'ReadParquetFileRule', 'WriteCSVFileRule', 'WriteParquetFileRule',
    'ReadJSONFileRule', 'WriteJSONFileRule',
    'ReadSQLQueryRule', 'WriteSQLTableRule',
]

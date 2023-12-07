import os

from .aggregate import AggregateRule
from etlrules.backends.common.basic import ProjectRule
from .basic import DedupeRule, ExplodeValuesRule, RenameRule, ReplaceRule, SortRule
from .concat import VConcatRule, HConcatRule
from .conditions import IfThenElseRule, FilterRule
from .datetime import (
    DateTimeLocalNowRule, DateTimeUTCNowRule, DateTimeToStrFormatRule,
    DateTimeRoundRule, DateTimeRoundDownRule, DateTimeRoundUpRule,
    DateTimeExtractComponentRule, DateTimeAddRule, DateTimeSubstractRule,
    DateTimeDiffRule,
)
from .fill import ForwardFillRule, BackFillRule
from .joins import LeftJoinRule, InnerJoinRule, OuterJoinRule, RightJoinRule
from .newcolumns import AddNewColumnRule, AddRowNumbersRule
from .numeric import AbsRule, RoundRule
from .strings import (
    StrLowerRule, StrUpperRule, StrCapitalizeRule, StrSplitRule, StrSplitRejoinRule,
    StrStripRule, StrPadRule, StrExtractRule,
)
from .types import TypeConversionRule

from etlrules.backends.common.basic import RulesBlock

## IO - extractors and loaders
from .io.files import ReadCSVFileRule, ReadParquetFileRule, WriteCSVFileRule, WriteParquetFileRule
from .io.db import ReadSQLQueryRule, WriteSQLTableRule

from .base import force_pyarrow_string_config

if os.environ.get("ETLRULES_ENABLE_PYARROW_STRING", "").lower() in ("1", "true", "yes"):
    force_pyarrow_string_config(True)
else:
    force_pyarrow_string_config(False)


__all__ = [
    'AggregateRule',
    'DedupeRule', 'ExplodeValuesRule', 'ProjectRule', 'RenameRule', 'ReplaceRule', 'SortRule',
    'VConcatRule', 'HConcatRule',
    'IfThenElseRule', 'FilterRule',
    'DateTimeLocalNowRule', 'DateTimeUTCNowRule', 'DateTimeToStrFormatRule',
    'DateTimeRoundRule', 'DateTimeRoundDownRule', 'DateTimeRoundUpRule',
    'DateTimeExtractComponentRule', 'DateTimeAddRule', 'DateTimeSubstractRule',
    'DateTimeDiffRule',
    'ForwardFillRule', 'BackFillRule',
    'LeftJoinRule', 'InnerJoinRule', 'OuterJoinRule', 'RightJoinRule',
    'AddNewColumnRule', 'AddRowNumbersRule',
    'AbsRule', 'RoundRule',
    'StrLowerRule', 'StrUpperRule', 'StrCapitalizeRule', 'StrSplitRule', 'StrSplitRejoinRule',
    'StrStripRule', 'StrPadRule', 'StrExtractRule',
    'TypeConversionRule',
    'RulesBlock',
    # IO extractors and loaders
    'ReadCSVFileRule', 'ReadParquetFileRule', 'WriteCSVFileRule', 'WriteParquetFileRule',
    'ReadSQLQueryRule', 'WriteSQLTableRule',
]

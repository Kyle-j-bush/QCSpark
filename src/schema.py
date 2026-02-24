from typing import Any
from pyspark.sql.types import StructType

def _validate_is_struct(arg_name: str, arg_value: Any) -> None:
    __tracebackhide__ = True
    if not isinstance(arg_value, StructType): 
        raise PySparkTypeError(
            errorClass = "NOT_STRUCT",
            messageParameters={"arg_name": arg_name, "arg_type": type(arg_value).__name__},
        )
def order_columns(schema: StructType) -> StructType:
    return StructType(sorted(schema, key= lambda x: x.name))

def rename_columns(schema: StructType) -> StructType:
    return StructType(
        [StructField(str(i), field.dataType, field.nullable) for i, field in enumerate(schema)]
    )

def check_schema_match(
    actual_schema: StructType,
    expected_schema: StructType,
    ignore_nulls: bool = True, 
    ignore_order: bool = False,
    ignore_column_name: bool = False
    ) -> bool:

    _validate_is_struct(actual_schema)
    _validate_is_struct(expected_schema)

    if ignore_column_order:
        actual_schema = order_columns(actual_schema)
        expected_schema = order_columns(expected_schema)
    if ignore_column_name:
        actual_schema = rename_columns(actual_schema)
        expected_schema = rename_columns(expected_schema)
    

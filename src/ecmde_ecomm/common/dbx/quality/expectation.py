from typing import Any

from pyspark.sql import Column
from pyspark.sql.functions import (
    col,
    lit,
    isnull,
    trim,
    length,
    when,
    raise_error,
)


def not_null(column: str | Column) -> Column:
    """
    Test to determine if a column in a dataframe is null. While you would typically declare a column as not null, this
    expectation is useful when testing a column that is conditionally nullable.
    """
    return when(
        isnull(column), raise_error(f"Column {_col_name(column)} cannot be not null")
    ).otherwise(_column(column))


def not_empty(column: str | Column, nullable: bool = True) -> Column:
    """
    Test to determine if a string column value is not empty. Not Empty includes empty string, and blank strings by
    default. Behavior can be overridden to include a null check by setting nullable=False.
    """
    return (
        when(
            lit(not nullable) & isnull(column),
            raise_error(lit(f"Column {_col_name(column)} cannot be null.")),
        )
        .when(
            length(trim(column)) == 0,
            raise_error(lit(f"Column {_col_name(column)} cannot be empty.")),
        )
        .otherwise(_column(column))
    )


def one_of(column: str | Column, accepted: list[Any]) -> Column:
    return when(_column(column).isin(accepted), _column(column)).otherwise(
        raise_error(f"Column {_col_name(column)} is not one of {",".join(accepted)}.")
    )


def _column(column: str | Column) -> Column:
    return column if isinstance(column, Column) else col(column)


def _col_name(col: str | Column) -> str:
    return col if isinstance(col, str) else col.name

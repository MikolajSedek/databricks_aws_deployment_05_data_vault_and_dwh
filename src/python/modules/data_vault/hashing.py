"""Data Vault 2.0 deterministic SHA-256 hash key and hashdiff Column expression builders."""

from typing import Final

from pyspark.sql import Column
from pyspark.sql import functions as F

DELIM: Final[str] = "||"
NULL_SEN: Final[str] = "^^"


def sha256_hash_key(*business_key_cols: str) -> Column:
    """Build a deterministic SHA-256 hash key Column expression from one or more business key columns.

    Applies Data Vault 2.0 normalization: coalesce to '^^', cast to string, trim, uppercase,
    and join multiple keys with delimiter '||'.

    Args:
        *business_key_cols: Business key column names in deterministic order.

    Returns:
        PySpark Column computing the 64-character hexadecimal SHA-256 digest.
    """
    normalized = [
        F.coalesce(F.upper(F.trim(F.col(col_name).cast("string"))), F.lit(NULL_SEN)) for col_name in business_key_cols
    ]
    return F.sha2(F.concat_ws(DELIM, *normalized), 256)


def sha256_hashdiff(*payload_cols: str) -> Column:
    """Build a deterministic SHA-256 hashdiff Column expression across descriptive Satellite attributes.

    Sorts payload columns alphabetically for order-independent determinism and preserves case.

    Args:
        *payload_cols: Descriptive attribute column names.

    Returns:
        PySpark Column computing the 64-character hexadecimal SHA-256 hashdiff.
    """
    sorted_cols = sorted(payload_cols)
    normalized = [F.coalesce(F.trim(F.col(col_name).cast("string")), F.lit(NULL_SEN)) for col_name in sorted_cols]
    return F.sha2(F.concat_ws(DELIM, *normalized), 256)

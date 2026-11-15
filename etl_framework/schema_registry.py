"""Schema registry: versioned dataset contracts loaded from YAML files.

Schemas live in a ``schemas/`` directory kept in Git alongside the pipeline
configs, one file per dataset::

    # schemas/silver/customers.yaml
    dataset: silver.customers
    strict: false            # optional: fail on extra columns too
    columns:
      - {name: customer_id, type: int, nullable: false}
      - {name: customer_name, type: string, nullable: true}

Read and write steps reference them with ``schema_ref: silver/customers``
(the file path under the schema base directory, extension optional). A
missing or invalid schema file fails validation before any step executes;
a DataFrame that does not match its contract fails the step at runtime.
"""

import os
import re

import yaml

from etl_framework.config_loader import load_yaml
from etl_framework.exceptions import SchemaContractError

_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LEADING_TYPE_NAME = re.compile(r"^[a-z]+")

# Aliases normalized to Spark's DataType.simpleString() vocabulary so
# contracts can say "integer" or "long" and still match.
_TYPE_ALIASES = {
    "integer": "int",
    "long": "bigint",
    "short": "smallint",
    "byte": "tinyint",
    "bool": "boolean",
    "str": "string",
    "character": "string",
    "varchar": "string",
    "numeric": "decimal",
    "dec": "decimal",
}


def normalize_type(type_string: str) -> str:
    """Normalize a type spelling for comparison (case, spaces, aliases)."""
    normalized = re.sub(r"\s+", "", str(type_string).lower())
    return _LEADING_TYPE_NAME.sub(
        lambda m: _TYPE_ALIASES.get(m.group(0), m.group(0)), normalized, count=1
    )


class SchemaContract:
    def __init__(self, ref: str, columns: list, dataset: str | None = None, strict: bool = False):
        self.ref = ref
        self.columns = columns  # list of {"name", "type", "nullable"} dicts
        self.dataset = dataset
        self.strict = strict

    def to_ddl(self) -> str:
        """DDL schema string usable with DataFrameReader.schema()."""
        return ", ".join(f"`{c['name']}` {c['type']}" for c in self.columns)

    def validate_dataframe(
        self, df, strict: bool | None = None, enforce_nullability: bool = False
    ) -> list:
        """Compare a DataFrame against this contract.

        Returns a list of human-readable problems; empty means the DataFrame
        satisfies the contract. Column matching is case-insensitive and
        order-independent. Extra columns are only reported when strict.
        Nullability is only checked when ``enforce_nullability`` is set,
        because Spark frequently reports readable sources as nullable.
        """
        strict = self.strict if strict is None else strict
        actual_fields = {field.name.lower(): field for field in df.schema.fields}
        problems = []
        expected_names = set()

        for column in self.columns:
            name = column["name"]
            expected_names.add(name.lower())
            field = actual_fields.get(name.lower())
            if field is None:
                problems.append(f"missing column '{name}' (expected type {column['type']})")
                continue
            expected_type = normalize_type(column["type"])
            actual_type = normalize_type(field.dataType.simpleString())
            if expected_type != actual_type:
                problems.append(
                    f"column '{name}' has type {field.dataType.simpleString()}, "
                    f"expected {column['type']}"
                )
            if enforce_nullability and column.get("nullable") is False and field.nullable:
                problems.append(f"column '{name}' must not be nullable")

        if strict:
            extra = sorted(
                field.name for key, field in actual_fields.items() if key not in expected_names
            )
            if extra:
                problems.append(f"unexpected extra column(s) not in contract: {', '.join(extra)}")
        return problems


class SchemaRegistry:
    """Loads and caches schema contracts from a base directory."""

    def __init__(self, base_path: str):
        self.base_path = base_path
        self._cache: dict[str, SchemaContract] = {}

    def _resolve_path(self, schema_ref: str) -> str:
        candidates = (
            [schema_ref]
            if schema_ref.endswith((".yaml", ".yml"))
            else [
                f"{schema_ref}.yaml",
                f"{schema_ref}.yml",
                schema_ref,
            ]
        )
        tried = []
        for candidate in candidates:
            path = os.path.join(self.base_path, candidate)
            tried.append(path)
            if os.path.isfile(path):
                return path
        raise SchemaContractError(schema_ref, f"schema file not found (tried: {', '.join(tried)})")

    def get(self, schema_ref: str) -> SchemaContract:
        if schema_ref in self._cache:
            return self._cache[schema_ref]

        path = self._resolve_path(schema_ref)
        try:
            with open(path, encoding="utf-8") as handle:
                raw = load_yaml(handle)
        except (OSError, yaml.YAMLError) as exc:
            raise SchemaContractError(schema_ref, f"could not load '{path}': {exc}") from exc

        contract = self._parse(schema_ref, raw)
        self._cache[schema_ref] = contract
        return contract

    @staticmethod
    def _parse(schema_ref: str, raw) -> SchemaContract:
        if (
            not isinstance(raw, dict)
            or not isinstance(raw.get("columns"), list)
            or not raw["columns"]
        ):
            raise SchemaContractError(
                schema_ref, "schema file must be a mapping with a non-empty 'columns' list"
            )
        columns = []
        for index, column in enumerate(raw["columns"]):
            position = f"columns[{index}]"
            if not isinstance(column, dict):
                raise SchemaContractError(schema_ref, f"{position} must be a mapping")
            name, type_string = column.get("name"), column.get("type")
            if not name or not _COLUMN_NAME.match(str(name)):
                raise SchemaContractError(schema_ref, f"{position} has a missing or invalid 'name'")
            if not type_string or not isinstance(type_string, str):
                raise SchemaContractError(
                    schema_ref, f"{position} ('{name}') has a missing or invalid 'type'"
                )
            columns.append(
                {
                    "name": str(name),
                    "type": type_string,
                    "nullable": column.get("nullable", True),
                }
            )
        return SchemaContract(
            ref=schema_ref,
            columns=columns,
            dataset=raw.get("dataset"),
            strict=bool(raw.get("strict", False)),
        )

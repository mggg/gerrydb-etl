"""Wrapper for direct database access via the CRUD server-side abstraction layer.

This is suitable only for very large bootstrapping/bulk import operations,
such as importing the entire PL 94-171 release. Its primary advantages are
the avoidance of serialization/REST/caching overhead, and--more importantly--
support for large-scale transactions.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from gerrydb_meta.crud import obj_meta
from gerrydb_meta.crud.column import COLUMN_TYPE_TO_VALUE_COLUMN
from gerrydb_meta.enums import ColumnType
from gerrydb_meta.models import ColumnValue, DataColumn, Geography, ObjectMeta, User
from gerrydb_meta.schemas import ObjectMetaCreate
from sqlalchemy import create_engine, insert, update, tuple_
from sqlalchemy.orm import Session, sessionmaker


@dataclass
class DirectTransactionContext:
    """Context for a direct database transaction."""

    db: Optional[Session] = None
    dry_run: bool = False
    notes: Optional[str] = None
    email: Optional[str] = None
    meta: Optional[ObjectMeta] = None
    user: Optional[User] = None

    def __enter__(self) -> "DirectTransactionContext":
        """Creates a write context with metadata."""
        if self.db is None:
            self.db = sessionmaker(create_engine(os.getenv("GERRYDB_DATABASE_URI")))()
        self.db.begin()

        if self.email is None:
            self.email = os.getenv("GERRYDB_EMAIL")

        if self.user is None:
            self.user = self.db.query(User).filter(User.email == self.email).first()

        if self.meta is None:
            self.meta = obj_meta.create(
                db=self.db,
                obj_in=ObjectMetaCreate(notes=self.notes),
                user=self.user,
            )

        return self  # TODO: cleanup

    def __exit__(self, exc_type, exc_value, traceback):
        if (
            exc_type
            or self.dry_run
            or os.getenv("GERRYDB_DRY_RUN", "").lower() == "true"
        ):
            self.db.rollback()
        else:
            try:
                self.db.commit()
            except Exception as ex:
                self.db.rollback()
                raise ex
        self.db.close()

    def load_column_values(
        self,
        *,
        cols: dict[str, DataColumn],
        geos: dict[str, Geography],
        df: pd.DataFrame,
    ) -> None:
        """Sets column values across geographies.

        Raises:
            ColumnValueTypeError: If column types do not match expected types.
        """
        now = datetime.now(timezone.utc)
        rows = []
        for col_name, col in cols.items():
            # Validate column data.
            val_column = COLUMN_TYPE_TO_VALUE_COLUMN[col.type]
            validation_errors = []
            values = df[col_name]
            for geo_id, value in values.items():
                geo = geos[geo_id]
                suffix = "column value for geography {geo.full_path}"
                if col.type == ColumnType.FLOAT and isinstance(value, int):
                    # Silently promote int -> float.
                    value = float(value)
                elif col.type == ColumnType.FLOAT and not isinstance(value, float):
                    validation_errors.append(
                        f"Expected integer or floating-point {suffix}"
                    )
                elif col.type == ColumnType.INT and not isinstance(value, int):
                    validation_errors.append(f"Expected integer {suffix}")
                elif col.type == ColumnType.STR and not isinstance(value, str):
                    validation_errors.append(f"Expected string {suffix}")
                elif col.type == ColumnType.BOOL and isinstance(value, bool):
                    validation_errors.append(f"Expected boolean {suffix}")
                rows.append(
                    {
                        "col_id": col.col_id,
                        "geo_id": geo.geo_id,
                        "meta_id": self.meta.meta_id,
                        "valid_from": now,
                        val_column: value,
                    }
                )

        if validation_errors:
            raise ValueError(errors=validation_errors)

        geo_ids = [geo.geo_id for geo in geos.values()]
        col_ids = [col.col_id for col in cols.values()]
        stale_values = []

        for col_id in col_ids:
            result = (
                self.db.query(ColumnValue.col_id, ColumnValue.geo_id)
                .filter(
                    ColumnValue.geo_id.in_(geo_ids),
                    ColumnValue.col_id == col_id,
                    ColumnValue.valid_to.is_(None),
                )
                .all()
            )
            stale_values.extend(result)

        with self.db.begin(nested=True):
            # Optimization: most column values are only set once, so we don't
            # need to invalidate old versions unless we previously detected them.
            if stale_values:
                stale_pairs = [(val.col_id, val.geo_id) for val in stale_values]
                self.db.execute(
                    update(ColumnValue)
                    .where(
                        tuple_(ColumnValue.col_id, ColumnValue.geo_id).in_(stale_pairs)
                    )
                    .values(valid_to=now)
                )
            self.db.execute(insert(ColumnValue), rows)

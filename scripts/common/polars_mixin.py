"""Mixin com utilitários de Delta Lake + Polars usados pelas camadas do ETL."""

import os
import json
from datetime import datetime
import polars as pl
import unidecode
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from scripts.common.config import DELTA_ROOT


class PolarsMixin:
    @staticmethod
    def ascii_upper(col: str) -> pl.Expr:
        """Expressão Polars que remove acentos e coloca em maiúsculas."""
        return pl.col(col).map_elements(
            lambda s: unidecode.unidecode(str(s)).upper() if s is not None else None,
            return_dtype=pl.Utf8,
        )

    def delta_path(self, *parts: str) -> str:
        """Monta o caminho de uma Delta table a partir da DELTA_ROOT."""
        return os.path.join(DELTA_ROOT, *parts)

    def delta_exists(self, path: str) -> bool:
        try:
            DeltaTable(path)
            return True
        except TableNotFoundError:
            return False

    def read_delta(self, path: str, columns=None) -> pl.DataFrame:
        """Lê uma Delta table como DataFrame Polars. Retorna vazio se não existir."""
        if not self.delta_exists(path):
            return pl.DataFrame()
        return pl.read_delta(path, columns=columns)

    def append_delta(self, df: pl.DataFrame, path: str) -> None:
        if df.is_empty():
            return
        os.makedirs(path, exist_ok=True)
        write_deltalake(path, df.to_arrow(), mode="append")

    def upsert_delta(self, df: pl.DataFrame, path: str, keys: list) -> None:
        """MERGE (upsert) de um DataFrame Polars numa Delta table pela chave `keys`.

        Cria a tabela caso ainda não exista.
        """
        if df.is_empty():
            return
        os.makedirs(path, exist_ok=True)
        arrow = df.to_arrow()
        if not self.delta_exists(path):
            write_deltalake(path, arrow, mode="overwrite")
            return
        predicate = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (
            DeltaTable(path)
            .merge(
                source=arrow,
                predicate=predicate,
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    def _state_path(self, name: str) -> str:
        return self.delta_path("_state", f"{name}.json")

    def get_watermark(self, name: str):
        path = self._state_path(name)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f).get("watermark")
        return None

    def set_watermark(self, name: str, value) -> None:
        path = self._state_path(name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump({"watermark": str(value)}, f)

    def read_incremental(
        self, path: str, watermark_name: str, ts_col: str = "created_at"
    ):
        """Lê de uma Delta table apenas as linhas com `ts_col` maior que o
        watermark salvo. Retorna (DataFrame, novo_watermark).
        """
        df = self.read_delta(path)
        if df.is_empty():
            return df, None
        wm = self.get_watermark(watermark_name)
        if wm:
            df = df.filter(pl.col(ts_col) > pl.lit(datetime.fromisoformat(wm)))
        new_wm = df.select(pl.col(ts_col).max()).item() if not df.is_empty() else None
        return df, new_wm

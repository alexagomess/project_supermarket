import re
import polars as pl
from datetime import datetime
from scripts.common.etl import BaseETL
from scripts.common.config import FOLDER_RAW


class ShoppingCleaned(BaseETL):
    """Camada cleaned dos itens comprados (Polars puro).

    Lê apenas os arquivos novos da raw (Google Drive) e grava numa Delta table
    local, controlando o incremental pela coluna `source_file`.
    """

    def __init__(self):
        super().__init__()
        self.folder_raw = FOLDER_RAW
        self.delta = self.delta_path("cleaned", "shopping")

    def _processed_files(self) -> set:
        df = self.read_delta(self.delta, columns=["source_file"])
        if df.is_empty():
            return set()
        return set(df["source_file"].unique().to_list())

    def _lookup(self, raw: pl.DataFrame, c0: str, c1: str, term: str):
        vals = (
            raw.filter(pl.col(c0).cast(pl.Utf8).str.contains(term, literal=True))
            .select(pl.col(c1).cast(pl.Utf8))
            .to_series()
            .to_list()
        )
        return vals[0] if vals else None

    def _parse_file(self, raw: pl.DataFrame, shopping_file: str):
        cols = raw.columns
        if len(cols) < 6:
            self.logger.error(f"Arquivo {shopping_file} com colunas insuficientes.")
            return None
        c0, c1, c2, c3, c4, c5 = cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]

        emission_date = self._lookup(raw, c0, c1, "Data Emissão")
        key_nfe = self._lookup(raw, c0, c1, "Chave de Acesso")
        if emission_date is None or key_nfe is None:
            self.logger.error(
                f"O arquivo {shopping_file} não contém data de emissão / chave."
            )
            return None
        key_nfe = re.sub(r"\D", "", key_nfe)
        reference_date = datetime.strptime(emission_date, "%d/%m/%Y %H:%M:%S")

        c2clean = (
            pl.col(c2)
            .cast(pl.Utf8)
            .str.replace_all('"', "")
            .str.replace_all("“", "")
            .str.replace_all("”", "")
        )
        descricao = (
            c2clean.str.split_exact("(Código:", 1)
            .struct.field("field_0")
            .str.replace_all(",", " ")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("descricao")
        )
        codigo = (
            c2clean.str.split_exact("(Código:", 1)
            .struct.field("field_1")
            .str.strip_chars(")")
            .alias("codigo")
        )
        quantidade = (
            pl.col(c3)
            .cast(pl.Utf8)
            .str.replace_all(r"[^\d.]", "")
            .cast(pl.Float64, strict=False)
            .alias("quantidade")
        )
        unidade = (
            pl.col(c4)
            .cast(pl.Utf8)
            .str.replace_all(r".*:", "")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("unidade")
        )
        valor_unitario = (
            pl.col(c5)
            .cast(pl.Utf8)
            .str.replace_all(r"[^\d,]", "")
            .str.replace_all(",", ".")
            .cast(pl.Float64, strict=False)
            .alias("valor_unitario")
        )

        out = raw.select(
            [
                pl.lit(key_nfe).alias("chave_de_acesso"),
                descricao,
                codigo,
                quantidade,
                unidade,
                valor_unitario,
                pl.lit(reference_date).alias("reference_date"),
            ]
        ).drop_nulls(
            subset=["descricao", "codigo", "quantidade", "unidade", "valor_unitario"]
        )

        if out.is_empty():
            return None

        now = datetime.now()
        return out.with_row_index("item_index").with_columns(
            [
                pl.col("item_index").cast(pl.Int64),
                pl.lit(now).alias("created_at"),
                pl.lit(now).alias("updated_at"),
                pl.lit(shopping_file).alias("source_file"),
            ]
        )

    def main(self):
        raw_files = self.read_google_drive(self.folder_raw) or []
        shopping_files = [f for f in raw_files if f.endswith("-shopping.csv")]
        processed = self._processed_files()
        new_files = [f for f in shopping_files if f not in processed]

        if not new_files:
            self.logger.info("Nenhum arquivo novo de shopping para processar.")
            return

        self.logger.info(f"{len(new_files)} novo(s) arquivo(s) de shopping.")
        frames = []
        for shopping_file in new_files:
            self.logger.info(f"Lendo o arquivo: {shopping_file}")
            raw = self.read_drive_csv_polars(self.folder_raw, shopping_file)
            if raw is None or raw.is_empty():
                self.logger.error(f"Arquivo {shopping_file} vazio ou não encontrado.")
                continue
            parsed = self._parse_file(raw, shopping_file)
            if parsed is not None and not parsed.is_empty():
                frames.append(parsed)

        if not frames:
            self.logger.info("Nenhum registro válido nos novos arquivos.")
            return

        df = pl.concat(frames, how="vertical_relaxed")
        self.append_delta(df, self.delta)
        self.logger.info(
            f"{df.height} linha(s) adicionada(s) à Delta cleaned/shopping."
        )

    def execute(self):
        try:
            self.main()
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    ShoppingCleaned().execute()

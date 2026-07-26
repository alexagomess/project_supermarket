import polars as pl
import pandas as pd
from datetime import datetime
from hashlib import sha256
from scripts.common.etl import BaseETL


class TrustedProducts(BaseETL):
    def __init__(self):
        super().__init__()
        self.source = self.delta_path("cleaned", "shopping")
        self.target = self.delta_path("trusted", "products")
        self.watermark = "trusted_products"
        self.keys = ["uid"]

    def _read_categorization(self) -> pl.DataFrame:
        pdf = pd.read_excel("scripts/raw/de_para_produtos.xlsx")
        pdf = pdf[["descricao", "tipo_produto", "marca", "categoria", "sub_categoria"]]
        pdf = pdf.astype(str).where(pd.notnull(pdf), None)
        return pl.from_pandas(pdf).unique(subset=["descricao"])

    def _read_ean(self):
        df = self.read_delta(self.delta_path("enrich", "ean"))
        if df.is_empty():
            return None
        return df.select(["codigo", "ean"]).unique(subset=["codigo"])

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now()
        base = (
            df.select(["descricao", "codigo"])
            .unique()
            .with_columns(
                pl.concat_str([pl.col("codigo"), pl.col("descricao")], separator="_")
                .map_elements(
                    lambda s: sha256(s.encode("utf-8")).hexdigest(),
                    return_dtype=pl.Utf8,
                )
                .alias("uid")
            )
            .with_columns(
                [
                    pl.lit(None, dtype=pl.Utf8).alias("descricao_completa"),
                    pl.lit(None, dtype=pl.Utf8).alias("ean"),
                    pl.lit(now).alias("created_at"),
                    pl.lit(now).alias("updated_at"),
                ]
            )
        )
        result = base.join(self._read_categorization(), on="descricao", how="left")
        ean_map = self._read_ean()
        if ean_map is not None:
            result = result.drop("ean").join(ean_map, on="codigo", how="left")
        return result

    def execute(self):
        try:
            self.run_delta_trusted()
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    TrustedProducts().execute()

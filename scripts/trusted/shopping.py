import polars as pl
from datetime import datetime
from hashlib import sha256
from scripts.common.etl import BaseETL


def _sha256(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


class TrustedShopping(BaseETL):
    def __init__(self):
        super().__init__()
        self.source = self.delta_path("cleaned", "shopping")
        self.target = self.delta_path("trusted", "shopping")
        self.watermark = "trusted_shopping"
        self.keys = ["uid"]

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now()
        return (
            df.with_columns(
                [
                    pl.concat_str(
                        [
                            pl.col("item_index").cast(pl.Utf8),
                            pl.col("codigo"),
                            pl.col("descricao"),
                            pl.col("reference_date").cast(pl.Utf8),
                            pl.col("chave_de_acesso"),
                        ],
                        separator="_",
                    )
                    .map_elements(_sha256, return_dtype=pl.Utf8)
                    .alias("uid"),
                    pl.concat_str(
                        [pl.col("codigo"), pl.col("descricao")], separator="_"
                    )
                    .map_elements(_sha256, return_dtype=pl.Utf8)
                    .alias("product_uid"),
                ]
            )
            .with_columns(
                [pl.lit(now).alias("created_at"), pl.lit(now).alias("updated_at")]
            )
            .select(
                [
                    "uid",
                    "item_index",
                    "product_uid",
                    "descricao",
                    "codigo",
                    "quantidade",
                    "unidade",
                    "valor_unitario",
                    "reference_date",
                    "chave_de_acesso",
                    "created_at",
                    "updated_at",
                ]
            )
        )

    def execute(self):
        try:
            self.run_delta_trusted()
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    TrustedShopping().execute()

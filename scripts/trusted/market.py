import polars as pl
from datetime import datetime
from scripts.common.etl import BaseETL


class TrustedMarket(BaseETL):
    def __init__(self):
        super().__init__()
        self.source = self.delta_path("cleaned", "nfe_information")
        self.target = self.delta_path("trusted", "market")
        self.watermark = "trusted_market"
        self.keys = ["cnpj"]

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now()
        return (
            df.select(["nome", "cnpj", "inscricao_estadual", "uf"])
            .unique(subset=["cnpj"])
            .with_columns(
                [
                    self.ascii_upper("nome").alias("nome"),
                    self.ascii_upper("uf").alias("uf"),
                    pl.lit(now).alias("created_at"),
                    pl.lit(now).alias("updated_at"),
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
    TrustedMarket().execute()

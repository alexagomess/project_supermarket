import polars as pl
from datetime import datetime
from scripts.common.etl import BaseETL

TEXT_UPPER = [
    "destino_da_operacao",
    "consumidor_final",
    "presenca_do_comprador",
]
SELECT_COLUMNS = [
    "cnpj",
    "destino_da_operacao",
    "consumidor_final",
    "presenca_do_comprador",
    "modelo",
    "serie",
    "numero",
    "data_emissao",
    "valor_total",
    "base_de_calculo_icms",
    "valor_icms",
    "protocolo",
    "chave_de_acesso",
]


class TrustedNFEInformation(BaseETL):
    def __init__(self):
        super().__init__()
        self.source = self.delta_path("cleaned", "nfe_information")
        self.target = self.delta_path("trusted", "nfe_information")
        self.watermark = "trusted_nfe_information"
        self.keys = ["chave_de_acesso"]

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now()
        return (
            df.select(SELECT_COLUMNS)
            .unique(subset=["chave_de_acesso"])
            .with_columns([self.ascii_upper(c).alias(c) for c in TEXT_UPPER])
            .with_columns(
                [
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
    TrustedNFEInformation().execute()

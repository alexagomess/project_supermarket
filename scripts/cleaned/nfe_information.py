import re
import polars as pl
import unidecode
from datetime import datetime
from scripts.common.etl import BaseETL
from scripts.common.config import FOLDER_RAW

RENAME_MAP = {
    "nome / razao social": "nome",
    "cnpj": "cnpj",
    "inscricao estadual": "inscricao_estadual",
    "uf": "uf",
    "destino da operacao": "destino_da_operacao",
    "consumidor final": "consumidor_final",
    "presenca do comprador": "presenca_do_comprador",
    "modelo": "modelo",
    "serie": "serie",
    "numero": "numero",
    "data emissao": "data_emissao",
    "valor total do servico": "valor_total",
    "base de calculo icms": "base_de_calculo_icms",
    "valor icms": "valor_icms",
    "protocolo": "protocolo",
    "chave de acesso": "chave_de_acesso",
}
TARGET_COLUMNS = list(dict.fromkeys(RENAME_MAP.values()))
CURRENCY_SOURCE = ["valor total do servico", "base de calculo icms", "valor icms"]
TEXT_COLUMNS = [
    "nome",
    "cnpj",
    "inscricao_estadual",
    "uf",
    "destino_da_operacao",
    "consumidor_final",
    "presenca_do_comprador",
    "modelo",
    "serie",
    "protocolo",
    "chave_de_acesso",
]


class NFEInformationCleaned(BaseETL):
    """Camada cleaned dos dados da nota fiscal (Polars puro).

    Lê apenas os arquivos novos da raw e grava numa Delta table local com
    colunas em snake_case, controlando o incremental por `source_file`.
    """

    def __init__(self):
        super().__init__()
        self.folder_raw = FOLDER_RAW
        self.delta = self.delta_path("cleaned", "nfe_information")

    def _processed_files(self) -> set:
        df = self.read_delta(self.delta, columns=["source_file"])
        if df.is_empty():
            return set()
        return set(df["source_file"].unique().to_list())

    @staticmethod
    def clean_currency(value):
        if isinstance(value, str):
            value = value.replace("R$", "").replace(".", "").replace(",", ".").strip()
            try:
                return float(value)
            except ValueError:
                return None
        return value

    def _parse_file(self, raw: pl.DataFrame, shopping_file: str):
        c0, c1 = raw.columns[0], raw.columns[1]
        sub = raw.select(
            [pl.col(c0).cast(pl.Utf8).alias("t"), pl.col(c1).cast(pl.Utf8).alias("v")]
        ).drop_nulls(subset=["v"])

        data = {}
        for titulo, valor in zip(sub["t"].to_list(), sub["v"].to_list()):
            if titulo is None:
                continue
            key = unidecode.unidecode(str(titulo)).lower().strip()
            data[key] = valor

        for col in CURRENCY_SOURCE:
            if col not in data:
                self.logger.error(
                    f"Erro: A coluna '{col}' não foi encontrada no arquivo {shopping_file}."
                )
                return None
            data[col] = self.clean_currency(data[col])

        row = {}
        for src, tgt in RENAME_MAP.items():
            if src not in data:
                self.logger.error(f"Coluna ausente '{src}' no arquivo {shopping_file}.")
                return None
            row[tgt] = data[src]

        row["cnpj"] = re.sub(r"\D", "", str(row["cnpj"]))
        row["chave_de_acesso"] = re.sub(r"\D", "", str(row["chave_de_acesso"]))

        out = pl.DataFrame({k: [v] for k, v in row.items()})
        out = out.with_columns(
            [
                pl.col("numero").cast(pl.Utf8).cast(pl.Int64, strict=False),
                pl.col("data_emissao")
                .cast(pl.Utf8)
                .str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
                pl.col("valor_total").cast(pl.Float64, strict=False),
                pl.col("base_de_calculo_icms").cast(pl.Float64, strict=False),
                pl.col("valor_icms").cast(pl.Float64, strict=False),
            ]
            + [pl.col(c).cast(pl.Utf8) for c in TEXT_COLUMNS]
        ).drop_nulls(subset=TARGET_COLUMNS)

        if out.is_empty():
            return None

        now = datetime.now()
        return out.with_columns(
            [
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
            self.logger.info("Nenhum arquivo novo de nfe_information para processar.")
            return

        self.logger.info(f"{len(new_files)} novo(s) arquivo(s) de nfe_information.")
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
            f"{df.height} linha(s) adicionada(s) à Delta cleaned/nfe_information."
        )

    def execute(self):
        try:
            self.main()
            self.logger.info("Processamento concluído com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    NFEInformationCleaned().execute()

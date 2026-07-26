import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
import polars as pl
from scripts.common.etl import BaseETL

NS = "{http://www.portalfiscal.inf.br/nfe}"
XML_DIR = os.getenv("EAN_XML_DIR", "data/xml")


class EANFromXML(BaseETL):
    """Enriquece o EAN (GTIN) dos produtos a partir dos XML das NF-e.

    Lê os XML da pasta `EAN_XML_DIR` (padrão data/xml), extrai o `cEAN` por
    `cProd`, grava um mapa incremental em Delta (enrich/ean) e atualiza
    products.ean no PostgreSQL. Ignora `SEM GTIN`.
    """

    def __init__(self):
        super().__init__()
        self.xml_dir = XML_DIR
        self.delta = self.delta_path("enrich", "ean")

    def _parse(self) -> dict:
        mapping = {}
        for path in sorted(glob.glob(os.path.join(self.xml_dir, "*.xml"))):
            try:
                root = ET.parse(path).getroot()
            except ET.ParseError as e:
                self.logger.error(f"XML inválido {path}: {e}")
                continue
            for prod in root.iter(f"{NS}prod"):
                cprod = prod.findtext(f"{NS}cProd")
                cean = prod.findtext(f"{NS}cEAN") or prod.findtext(f"{NS}cEANTrib")
                if not cprod or not cean:
                    continue
                cean = cean.strip()
                if not cean.isdigit():
                    continue
                mapping[str(cprod).strip()] = cean
        return mapping

    def _update_postgres(self, df: pl.DataFrame):
        query = self.text(
            "UPDATE products SET ean = :ean, updated_at = :updated_at "
            "WHERE codigo = :codigo AND (ean IS NULL OR ean <> :ean)"
        )
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(query, df.to_dicts())

    def execute(self):
        try:
            mapping = self._parse()
            if not mapping:
                self.logger.info("Nenhum EAN encontrado nos XML.")
                return
            now = datetime.now()
            df = pl.DataFrame(
                {"codigo": list(mapping.keys()), "ean": list(mapping.values())}
            ).with_columns(pl.lit(now).alias("updated_at"))
            self.upsert_delta(df, self.delta, ["codigo"])
            self._update_postgres(df)
            self.logger.info(f"{df.height} EAN(s) enriquecido(s).")
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


if __name__ == "__main__":
    EANFromXML().execute()

"""Camada de carga: lê as Delta tables trusted (incrementalmente) e faz upsert
no PostgreSQL, que serve como camada de consulta/serving."""

from scripts.common.etl import BaseETL


class _PostgresLoad(BaseETL):
    source_table = None
    table_name = None
    watermark = None
    columns: list = []
    conflict_columns: list = []
    update_columns: list = []

    def __init__(self):
        super().__init__()
        self.source = self.delta_path("trusted", self.source_table)

    def execute(self):
        try:
            df, new_wm = self.read_incremental(
                self.source, self.watermark, ts_col="updated_at"
            )
            if df.is_empty():
                self.logger.info(
                    f"Sem novos registros na trusted/{self.source_table} para carregar."
                )
                return
            self.upsert_postgres(
                df,
                self.table_name,
                self.columns,
                self.conflict_columns,
                self.update_columns,
            )
            if new_wm is not None:
                self.set_watermark(self.watermark, new_wm)
        except Exception as e:
            self.logger.error(f"Erro durante o processamento: {e}")
            raise e


class LoadMarket(_PostgresLoad):
    source_table = "market"
    table_name = "market"
    watermark = "load_market"
    columns = ["nome", "cnpj", "inscricao_estadual", "uf", "created_at", "updated_at"]
    conflict_columns = ["cnpj"]
    update_columns = ["updated_at"]


class LoadNFEInformation(_PostgresLoad):
    source_table = "nfe_information"
    table_name = "nfe_information"
    watermark = "load_nfe_information"
    columns = [
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
        "created_at",
        "updated_at",
    ]
    conflict_columns = ["chave_de_acesso"]
    update_columns = ["updated_at"]


class LoadProducts(_PostgresLoad):
    source_table = "products"
    table_name = "products"
    watermark = "load_products"
    columns = [
        "uid",
        "codigo",
        "descricao",
        "descricao_completa",
        "marca",
        "categoria",
        "sub_categoria",
        "tipo_produto",
        "ean",
        "created_at",
        "updated_at",
    ]
    conflict_columns = ["uid"]
    update_columns = [
        "descricao",
        "descricao_completa",
        "marca",
        "categoria",
        "sub_categoria",
        "tipo_produto",
        "updated_at",
    ]


class LoadShopping(_PostgresLoad):
    source_table = "shopping"
    table_name = "shopping"
    watermark = "load_shopping"
    columns = [
        "uid",
        "item_index",
        "product_uid",
        "descricao",
        "codigo",
        "quantidade",
        "unidade",
        "valor_unitario",
        "reference_date",
        "created_at",
        "updated_at",
        "chave_de_acesso",
    ]
    conflict_columns = ["uid"]
    update_columns = [
        "product_uid",
        "descricao",
        "reference_date",
        "quantidade",
        "unidade",
        "valor_unitario",
        "updated_at",
    ]

"""DAG de orquestração do pipeline de supermercado.

Trigger manual com um único parâmetro (`nfe_key`, URL completa ou chave).
Arquitetura incremental em camadas:
raw (CSV no Drive) -> cleaned (Delta) -> trusted (Delta) -> postgres (serving).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def _run_raw_web_scraping(**context):
    from scripts.raw.web_scraping import run

    run(context["params"]["nfe_key"])


def _run_cleaned_nfe_information(**_):
    from scripts.cleaned.nfe_information import NFEInformationCleaned

    NFEInformationCleaned().execute()


def _run_cleaned_shopping(**_):
    from scripts.cleaned.shopping import ShoppingCleaned

    ShoppingCleaned().execute()


def _run_trusted_market(**_):
    from scripts.trusted.market import TrustedMarket

    TrustedMarket().execute()


def _run_trusted_nfe_information(**_):
    from scripts.trusted.nfe_information import TrustedNFEInformation

    TrustedNFEInformation().execute()


def _run_trusted_products(**_):
    from scripts.trusted.products import TrustedProducts

    TrustedProducts().execute()


def _run_trusted_shopping(**_):
    from scripts.trusted.shopping import TrustedShopping

    TrustedShopping().execute()


def _run_load_market(**_):
    from scripts.load.postgres import LoadMarket

    LoadMarket().execute()


def _run_load_nfe_information(**_):
    from scripts.load.postgres import LoadNFEInformation

    LoadNFEInformation().execute()


def _run_load_products(**_):
    from scripts.load.postgres import LoadProducts

    LoadProducts().execute()


def _run_load_shopping(**_):
    from scripts.load.postgres import LoadShopping

    LoadShopping().execute()


def _run_enrich_ean(**_):
    from scripts.enrich.ean_from_xml import EANFromXML

    EANFromXML().execute()


with DAG(
    dag_id="supermarket",
    description="Pipeline incremental raw -> cleaned -> trusted -> postgres.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["supermarket", "etl"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "nfe_key": Param(
            "",
            type="string",
            title="URL ou chave da NF-e",
            description="Cole a URL completa da NF-e ou a chave de acesso (44 dígitos).",
        )
    },
) as dag:

    with TaskGroup(group_id="raw") as raw:
        web_scraping = PythonOperator(
            task_id="web_scraping",
            python_callable=_run_raw_web_scraping,
            execution_timeout=timedelta(minutes=10),
        )

    with TaskGroup(group_id="cleaned") as cleaned:
        cleaned_nfe_information = PythonOperator(
            task_id="nfe_information",
            python_callable=_run_cleaned_nfe_information,
        )
        cleaned_shopping = PythonOperator(
            task_id="shopping",
            python_callable=_run_cleaned_shopping,
        )

    with TaskGroup(group_id="trusted") as trusted:
        trusted_market = PythonOperator(
            task_id="market", python_callable=_run_trusted_market
        )
        trusted_nfe_information = PythonOperator(
            task_id="nfe_information", python_callable=_run_trusted_nfe_information
        )
        trusted_products = PythonOperator(
            task_id="products", python_callable=_run_trusted_products
        )
        trusted_shopping = PythonOperator(
            task_id="shopping", python_callable=_run_trusted_shopping
        )

    with TaskGroup(group_id="enrich") as enrich:
        enrich_ean = PythonOperator(
            task_id="ean", python_callable=_run_enrich_ean
        )

    with TaskGroup(group_id="load") as load:
        load_market = PythonOperator(task_id="market", python_callable=_run_load_market)
        load_nfe_information = PythonOperator(
            task_id="nfe_information", python_callable=_run_load_nfe_information
        )
        load_products = PythonOperator(
            task_id="products", python_callable=_run_load_products
        )
        load_shopping = PythonOperator(
            task_id="shopping", python_callable=_run_load_shopping
        )

    web_scraping >> [cleaned_nfe_information, cleaned_shopping]

    cleaned_nfe_information >> [trusted_market, trusted_nfe_information]
    cleaned_shopping >> [trusted_products, trusted_shopping]

    web_scraping >> enrich_ean >> trusted_products

    trusted_market >> load_market
    trusted_nfe_information >> load_nfe_information
    trusted_products >> load_products
    trusted_shopping >> load_shopping

    load_market >> load_nfe_information >> load_shopping
    load_products >> load_shopping

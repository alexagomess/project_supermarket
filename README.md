# Projeto de Automação de Dados de Supermercado

Projeto pessoal para responder, a partir das notas fiscais das compras: **já comprei este produto antes? estava mais barato? em qual mercado?**

O fluxo processa as NF-e em três camadas (arquitetura *medallion*):

- **raw** — dados originais extraídos por web scraping do portal da NF-e.
- **cleaned** — dados limpos e padronizados.
- **trusted** — dados confiáveis, prontos para análise, gravados no PostgreSQL.

A orquestração é feita com **Apache Airflow** (via Docker Compose) e os arquivos intermediários ficam no **Google Drive**.

## Estrutura do projeto

```
project_supermarket/
├── dags/                        # DAGs do Airflow
├── config/                      # airflow.cfg
├── diagram/                     # modelo de dados (dbdiagram)
├── docker-compose.yaml          # Airflow + PostgreSQL + Redis
├── requirements.txt
└── scripts/
    ├── common/
    │   ├── config.py            # variáveis de ambiente / conexões
    │   ├── etl.py               # BaseETL (Drive, PostgreSQL, hashes, upsert, pipeline trusted)
    │   └── logging.py           # Logger
    ├── docs/
    │   └── oath_gdrive.py       # autenticação Google Drive
    ├── raw/
    │   ├── web_scraping.py      # extração da NF-e
    │   └── de_para_produtos.xlsx
    ├── cleaned/
    │   ├── shopping.py          # limpeza dos itens comprados
    │   └── nfe_information.py    # limpeza dos dados da nota
    └── trusted/
        ├── shopping.py
        ├── products.py
        ├── market.py
        ├── nfe_information.py
        ├── supermarket.py       # persistência dos CSVs trusted no Drive
        ├── indicators/          # consultas SQL de indicadores
        └── schemas/
```

## Modelo de dados

Quatro tabelas relacionadas por `cnpj`, `chave_de_acesso` e `codigo`:

- `market` (mercados) — PK `cnpj`
- `nfe_information` (dados da nota) — PK `chave_de_acesso`
- `products` (produtos) — PK `uid`
- `shopping` (itens comprados) — PK `uid`

Detalhes em [diagram/diagram.txt](diagram/diagram.txt).

## Pré-requisitos

- Python 3.x e pip
- Docker e Docker Compose (para o Airflow)

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração do Google Drive

1. No [Google Cloud Console](https://console.cloud.google.com/), crie um projeto e ative a **Google Drive API**.
2. Em **Credenciais**, crie um **ID do cliente OAuth** do tipo **Aplicativo para computador** e baixe o `client_secrets.json` para a raiz do projeto.
3. Gere o `token.json` autenticando uma vez:

   ```python
   from scripts.docs.oath_gdrive import authenticate
   authenticate()  # abre o fluxo OAuth e gera o token.json
   ```

## Configuração do `.env`

Crie um arquivo `.env` na raiz com:

```bash
# Pastas do Google Drive (IDs)
FOLDER_RAW=
FOLDER_CLEANED=
FOLDER_CLEANED_SHOPPING=
FOLDER_CLEANED_NFE_INFORMATION=
FOLDER_TRUSTED=
FOLDER_TRUSTED_SHOPPING=
FOLDER_TRUSTED_NFE_INFORMATION=
FOLDER_TRUSTED_PRODUCTS=
FOLDER_TRUSTED_MARKET=

# PostgreSQL local da aplicação (serviço postgres-app do docker-compose)
DB_HOST=postgres-app
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_NAME=supermarket
```

> O PostgreSQL da aplicação roda como o serviço `postgres-app` no Docker e é
> inicializado automaticamente com o schema de [diagram/diagram.sql](diagram/diagram.sql).
> A porta `5432` é exposta no host para acesso via `psql`/DBeaver.
> Para rodar os scripts direto no host (fora do Docker), use `DB_HOST=localhost`.

## Executando o pipeline

Cada camada pode ser executada individualmente:

```bash
# raw — extrai a NF-e (informe a chave de acesso de 44 dígitos)
python scripts/raw/web_scraping.py

# cleaned
python scripts/cleaned/shopping.py
python scripts/cleaned/nfe_information.py

# trusted (grava no PostgreSQL)
python scripts/trusted/shopping.py
python scripts/trusted/products.py
python scripts/trusted/market.py
python scripts/trusted/nfe_information.py
```

### Airflow (orquestração)

O runtime roda em containers Docker. As dependências dos scripts são instaladas
numa imagem customizada ([Dockerfile](Dockerfile) + [requirements-docker.txt](requirements-docker.txt)).
O `.env`, o `token.json` e o `client_secrets.json` da raiz são montados nos containers.

```bash
make build   # constrói a imagem customizada do Airflow
make up      # sobe Airflow + PostgreSQL + Redis
make logs    # acompanha os logs
make down    # derruba os serviços
```

A interface fica disponível em `http://localhost:8081` (usuário/senha padrão: `airflow`/`airflow`).

#### DAG `supermarket`

Orquestra as três camadas. É **acionada manualmente** e recebe um único parâmetro,
`nfe_key` (chave de acesso da NF-e de 44 dígitos):

```
raw.web_scraping
  ├─ cleaned.nfe_information ─┬─ trusted.market
  │                          ├─ trusted.nfe_information
  │                          └─ trusted.supermarket
  └─ cleaned.shopping ───────┬─ trusted.products
                             ├─ trusted.shopping
                             └─ trusted.supermarket
```

## BI (Metabase)

O [Metabase](https://www.metabase.com/) sobe junto com o Docker e conecta ao
PostgreSQL da aplicação pela rede interna do compose (não precisa expor o banco).

```bash
docker compose up -d metabase
```

Acesse `http://localhost:3000`, crie a conta de admin e adicione a conexão:

| Campo | Valor |
|---|---|
| Tipo | PostgreSQL |
| Host | `postgres-app` |
| Porta | `5432` |
| Database | `supermarket` |
| Usuário / Senha | `postgres` / `postgres` |

Views analíticas prontas ([diagram/views.sql](diagram/views.sql)) para os dashboards:

- `vw_compras` — base achatada (item a item) com produto, mercado, data e valores.
- `vw_gasto_por_categoria` — gasto e quantidade por categoria / tipo de produto.
- `vw_preco_por_mercado` — preço mín/médio/máx do produto por mercado.
- `vw_recorrencia` — frequência de compra por produto (recorrência).
- `vw_gasto_mensal` — gasto por mês e por mercado (comportamento no tempo).

## Agradecimentos

Agradeço a Deus pela oportunidade de desenvolver este projeto e à minha esposa pelo apoio e incentivo.

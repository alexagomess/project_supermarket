##### Portuguese after english
# Supermarket Data Automation Project

This project was developed to meet a personal need of mine and my wife when we go to the supermarket: to know if we have already bought a certain product before, if it was cheaper and in which market.

#### The project so far
The project flow begins with the processing of the invoices of the purchases made. This data is organized and transformed into three distinct layers: raw, cleaned and trusted. The raw layer stores the original data, the cleaned layer contains processed and cleaned data, while the trusted layer is intended for data that can be trusted and used for analysis and reports.

#### Next Steps
The next steps of the project include the implementation of Apache Airflow to orchestrate the updating of data between layers, ensuring that the information is always up to date. A connection to a PostgreSQL database is also planned to allow queries and analysis of the data through SQL. Finally, I intend to integrate a data visualization tool, such as Looker Studio or Power BI, to present insights in an intuitive and accessible way.

## Project Structure

```
project_supermarket/
├── cleaned/
│ ├── __init__.py
│ ├── products.py
│ ├── shopping.py
│ └── nfe_info.py
├── docs/
│ ├── __init__.py
│ ├── authentication.py
│ ├── gdrive_read.py
│ ├── gdrive_write.py
│ └── write_dataframe.py
├── raw/
│ ├── __init__.py
│ ├── web_scrapping_products.py
│ └── web_scrapping_table_nfe.py
├── trusted/
│ └── (files generated in the trusted layer)
├── .env
└── requirements.txt
```

## Prerequisites

Make sure you have the following installed:

- Python 3.x
- pip

### Dependencies

To install the project dependencies, run:

```bash
pip install -r requirements.txt
```

#### Google Drive Setup
To authenticate your project with Google Drive and allow it to read and write files, follow the steps below:

#### 1. Create a Project on Google Cloud
Access the Google Cloud Console.
Create a new project.
Enable the Google Drive API for your project.

#### 2. Create Credentials
Go to the "Credentials" section in the left-hand menu.
Click "Create Credentials" and choose "OAuth Client ID".
Select "Desktop Application" as the application type.
Click "Create" and download the client_secrets.json file.

#### 3. Authenticate and Generate Token
##### 1. Place the client_secrets.json file in the root of your project.
##### 2. Run the following script to generate the token.json file, which will be used for future authentications:
```bash
from docs.authentication import authenticate_gdrive
authenticate_gdrive() # This script will perform the authentication and generate the token.json
```

#### 4. .env configuration
Create a .env file in the root of your project with the following variables:

```bash
FOLDER_RAW=Folder ID
FOLDER_CLEANED=Folder ID
FOLDER_TRUSTED=Folder ID
FOLDER_TRUSTED_SHOPPING=Folder ID
FOLDER_TRUSTED_NFE_INFORMATION=Folder ID
FOLDER_TRUSTED_PRODUCTS=Folder ID
FOLDER_TRUSTED_MARKET=Folder ID
```

### Running the Project
After configuring the credentials and the environment, you can run the main scripts to process the data:

To process the purchase data (shopping):
```bash
python cleaned/shopping.py
```
To process the invoice data (nfe_info):
```bash
python cleaned/nfe_info.py
```

### Notes
Make sure that the libraries mentioned in the requirements.txt are installed correctly.

The code is structured in three layers: raw, cleaned and trusted, allowing an organized and clear data flow.

### Acknowledgements
First of all, I would like to thank God for allowing and enabling me to develop this personal project. I would also like to thank my wife for supporting me and encouraging me to continue working on this, even on the most tiring days.



#
##### Em português
# Projeto de Automação de Dados de Supermercado

Este projeto foi desenvolvido para atender a uma necessidade pessoal minha e da minha esposa ao frequentarmos o mercado: saber se já compramos um determinado produto antes, se ele estava mais barato e em qual mercado.

#### O projeto até agora
O fluxo do projeto inicia-se com o processamento das notas fiscais das compras realizadas. Esses dados são organizados e transformados em três camadas distintas: raw, cleaned e trusted. A camada raw armazena os dados originais, a camada cleaned contém dados processados e limpos, enquanto a camada trusted é destinada a dados que podem ser confiáveis e utilizados para análises e relatórios.

#### Próximos Passos
Os próximos passos do projeto incluem a implementação do Apache Airflow para orquestrar a atualização dos dados entre as camadas, garantindo que as informações estejam sempre atualizadas. Também está planejada uma conexão com um banco de dados PostgreSQL para permitir consultas e análises dos dados por meio de SQL. Finalmente, pretendo integrar uma ferramenta de visualização de dados, como Looker Studio ou Power BI, para apresentar insights de forma intuitiva e acessível.

## Estrutura do Projeto

```
project_supermarket/
├── cleaned/
│   ├── __init__.py
│   ├── products.py
│   ├── shopping.py
│   └── nfe_info.py
├── docs/
│   ├── __init__.py
│   ├── autentication.py
│   ├── gdrive_read.py
│   ├── gdrive_write.py
│   └── write_dataframe.py
├── raw/
│   ├── __init__.py
│   ├── web_scrapping_products.py
│   └── web_scrapping_table_nfe.py
├── trusted/
│   └── (arquivos gerados na camada trusted)
├── .env
└── requirements.txt
```


## Pré-requisitos

Certifique-se de ter o seguinte instalado:

- Python 3.x
- pip

### Dependências

Para instalar as dependências do projeto, execute:

```bash
pip install -r requirements.txt
```

#### Configuração do Google Drive
Para autenticar seu projeto com o Google Drive e permitir que ele leia e escreva arquivos, siga os passos abaixo:

#### 1. Criar um Projeto no Google Cloud
Acesse o Google Cloud Console.
Crie um novo projeto.
Ative a Google Drive API para seu projeto.

#### 2. Criar Credenciais
Vá até a seção "Credenciais" no menu do lado esquerdo.
Clique em "Criar credenciais" e escolha "ID do cliente OAuth".
Selecione "Aplicativo de desktop" como o tipo de aplicativo.
Clique em "Criar" e baixe o arquivo client_secrets.json.

#### 3. Autenticar e Gerar o Token
##### 1. Coloque o arquivo client_secrets.json na raiz do seu projeto.
##### 2. Execute o seguinte script para gerar o arquivo token.json, que será usado para autenticações futuras:
```bash
from docs.autentication import authenticate_gdrive
authenticate_gdrive()  # Este script fará a autenticação e gerará o token.json
```

#### 4. Configuração do .env
Crie um arquivo .env na raiz do seu projeto com as seguintes variáveis:

```bash
FOLDER_RAW=ID da folder
FOLDER_CLEANED=ID da folder
FOLDER_TRUSTED=ID da folder
FOLDER_TRUSTED_SHOPPING=ID da folder
FOLDER_TRUSTED_NFE_INFORMATION=ID da folder
FOLDER_TRUSTED_PRODUCTS=ID da folder
FOLDER_TRUSTED_MARKET=ID da folder
```


### Execução do Projeto
Após configurar as credenciais e o ambiente, você pode executar os scripts principais para processar os dados:

Para processar os dados de compras (shopping):
```bash
python cleaned/shopping.py
```
Para processar os dados de notas fiscais (nfe_info):
```bash
python cleaned/nfe_info.py
```

### Observações
Certifique-se de que as bibliotecas mencionadas no requirements.txt estejam instaladas corretamente.
O código está estruturado em três camadas: raw, cleaned e trusted, permitindo um fluxo de dados organizado e claro.


### Agradecimentos
Agradeço primeiramente a Deus por me permitir e me capacitar desenvolver esse projeto pessoal. Também agradeço a minha esposa por me apoiar e me incentivar em continuar trabalhando nisso, mesmo nos dias mais cansativos. 
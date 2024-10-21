import requests
from bs4 import BeautifulSoup

nfe_key = '31241004737552002262651090001641671801665264'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

# Fazendo a requisição para a página
page = requests.get(f'https://portalsped.fazenda.mg.gov.br/portalnfce/sistema/qrcode.xhtml?p={nfe_key}%7C2%7C1%7C1%7C8B75E1F34CEEB8DE8D620838EBB3E1E845379697', headers=headers)

# Parsing do conteúdo HTML
soup = BeautifulSoup(page.content, 'html.parser')

# Encontrando todos os elementos com a classe 'panel panel-default'
panels = soup.find_all('div', class_='panel panel-default')

# Verificando se existem pelo menos quatro elementos
if len(panels) >= 4:
    # Acessando a quarta ocorrência
    fourth_panel = panels[3]
    # Extraindo o conteúdo do quarto painel
    panel_content = fourth_panel.get_text(strip=True)
    print(panel_content)
else:
    print("Menos de quatro elementos encontrados.")
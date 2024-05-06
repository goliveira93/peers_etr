import requests
from bs4 import BeautifulSoup
import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import chardet
import re

# Configuração do site e diretório
url_base = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
diretorio = "C://programs//compara_peers_etrnty//data"

# Criar diretório se não existir
if not os.path.exists(diretorio):
    os.makedirs(diretorio)

# Baixar o arquivo
response = requests.get(url_base)
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    all_zip_files = soup.find_all("a", href=True)

    # Inverter para pegar o último arquivo
    for zip_file in reversed(all_zip_files):
        href = zip_file['href']
        text = zip_file.next_sibling
        if text and '2024' in href:
            size_match = re.search(r'(\d+)M', text)
            if size_match and int(size_match.group(1)) >= 15:
                download_link = url_base + href
                response = requests.get(download_link)
                if response.status_code == 200:
                    filepath = os.path.join(diretorio, href)
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    print(f"Arquivo {href} baixado com sucesso.")

                    # Descompactar apenas os arquivos necessários
                    with zipfile.ZipFile(filepath, 'r') as zip_ref:
                        for member in zip_ref.infolist():
                            if 'cda_fi_BLC_2' in member.filename or 'cda_fi_PL' in member.filename:
                                zip_ref.extract(member, diretorio)
                    print(f"Arquivos extraídos com sucesso.")

                    # Se chegou até aqui, sair do loop para não baixar mais nenhum arquivo
                    break

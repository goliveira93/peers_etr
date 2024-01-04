import json
from db_functions import fetch_all_fundos
import pandas as pd

sinonimos = fetch_all_fundos()

# Cria um dicionário com os sinônimos
dicionario_sinonimos = {item[0]: item[1] for item in sinonimos}

# # Define o caminho completo para o arquivo Excel incluindo o nome do arquivo e sua extensão
# caminho_arquivo_excel = "C:\\Users\\Gustavo.Oliveira\\Documents\\GitHub\\python_etr\\tools\\dataset_cvm\\scripts_cvm\\cvm_fia\\gráficos\\sinonimos.xlsx"

# # Cria um DataFrame com os sinônimos
# df_sinonimos = pd.DataFrame(list(dicionario_sinonimos.items()), columns=[
#                             'Nome Original', 'Nome Substituto'])

# # Salva o DataFrame como um arquivo Excel
# df_sinonimos.to_excel(caminho_arquivo_excel, index=False)

# print(f"Dicionário de sinônimos salvo em {caminho_arquivo_excel} com sucesso!")

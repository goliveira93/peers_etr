import pandas as pd
from libs.delta_etrnty import gera_df

fundos = ["EON", "EVO"]
periodo = "YTD"

# Lista para armazenar os dataframes gerados
dfs = []

for fundo in fundos:
    df = gera_df(fundo, periodo)
    dfs.append(df)

# Concatenar todos os dataframes em um único dataframe
df_concat = pd.concat(dfs)

# Selecionar as colunas desejadas e remover duplicados
df_unique = df_concat[['CNPJ_FUNDO_COTA', 'NM_FUNDO_COTA', 'CNPJ_FUNDO']].drop_duplicates()

# Salvar o resultado em um arquivo Excel
df_unique.to_excel('fundos_unicos.xlsx', index=False)

print("Arquivo Excel gerado com sucesso!")
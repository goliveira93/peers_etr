import pandas as pd
from datetime import datetime, timedelta
from workalendar.america import Brazil
from et_lib.ET_Data_Reader import QuantumHistoricalData
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, Date, Text
from pandas.tseries.offsets import MonthEnd
from pandas.tseries.offsets import BMonthEnd
import json
from dotenv import load_dotenv
import os


def ajustar_data(data, mes_vigente=10):
    if data.month == mes_vigente:
        offset = -2
    else:
        offset = 0

    bme = BMonthEnd()
    data_ajustada = bme.rollforward(data)

    while offset != 0:
        data_ajustada -= timedelta(days=1)
        if calendario.is_working_day(data_ajustada):
            offset += 1 if offset < 0 else -1

    return data_ajustada


# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém as variáveis de ambiente
user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port")
database = os.getenv("database")

# Usa as variáveis para criar a conexão
engine = create_engine(
    f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")

query = "SELECT * FROM cvm_peers_fia WHERE Gestor NOT LIKE 'Etrnty'"
df = pd.read_sql(query, con=engine)


# Adicione esta linha logo após ler o DataFrame df
colunas_originais = df.columns.tolist()

calendario = Brazil()

df['DT_COMPTC'] = df['DT_COMPTC'].apply(ajustar_data)

hoje = datetime.now()
ultimo_dia_mes_anterior = datetime(2023, 9, 30)  # Seu último ponto de dados é 30/09/2023

hoje = datetime.now()
ultimo_dia_mes_anterior = datetime(2023, 9, 30)  # Seu último ponto de dados é 30/09/2023

datas_desejadas = []
for mes in range(10, 13):  # Loop de outubro (10) a dezembro (12)
    # Ajuste para obter o último dia de cada mês corretamente
    if mes == 12:
        ultimo_dia_mes = datetime(ultimo_dia_mes_anterior.year, mes, 31)
    else:
        ultimo_dia_mes = datetime(ultimo_dia_mes_anterior.year, mes + 1, 1) - timedelta(days=1)
    
    print(f"Iniciando checagem para o mês: {mes}, data inicial: {ultimo_dia_mes}")  # Debug
    while not calendario.is_working_day(ultimo_dia_mes) or \
            (ultimo_dia_mes - timedelta(days=1)).day == 1:
        ultimo_dia_mes -= timedelta(days=1)
        print(f"Checando data (loop while): {ultimo_dia_mes}")  # Debug
    datas_desejadas.append(ultimo_dia_mes)
    print(f"Data adicionada: {ultimo_dia_mes}")  # Confirmação da data adicionada

# Verifique os prints para confirmar se as datas estão sendo calculadas corretamente


novas_linhas = []
ultimas_linhas = df[df['DT_COMPTC'] == df['DT_COMPTC'].max()]
for _, linha in ultimas_linhas.iterrows():
    for data in datas_desejadas:
        if data > df['DT_COMPTC'].max() + timedelta(days=1):
            nova_linha = linha.copy()
            nova_linha['DT_COMPTC'] = data
            novas_linhas.append(nova_linha)

df = pd.concat([df, pd.DataFrame(novas_linhas)], ignore_index=True)

df['DT_COMPTC'] = df['DT_COMPTC'] + BMonthEnd(0)


fundos = pd.concat([df["CNPJ_FUNDO"], df["CNPJ_FUNDO_COTA"]]).unique().tolist()
start_date = datetime.strptime("06-30-2022", "%m-%d-%Y")
end_date = datetime.strptime("03-01-2024", "%m-%d-%Y")
q = QuantumHistoricalData(start_date, end_date, fundos, ["PX_LAST"], "MONTHLY")
precos = q.getData()
precos = precos.droplevel(1, axis=1)

retornos = precos / precos.shift(1) - 1
retornos.reset_index(inplace=True)
retornos.rename(columns={'index': 'DT_COMPTC'}, inplace=True)

ret_melt_cota = retornos.melt(
    id_vars=['DT_COMPTC'], var_name='CNPJ_FUNDO_COTA', value_name='RETORNO')
ret_melt_fundo = retornos.melt(
    id_vars=['DT_COMPTC'], var_name='CNPJ_FUNDO', value_name='RETORNO_PEER')

# Faça a primeira fusão
df = pd.merge(df, ret_melt_cota, on=[
              'DT_COMPTC', 'CNPJ_FUNDO_COTA'], how='left', suffixes=('', '_novo'))

# Substitua os dados originais e exclua a coluna nova
df['RETORNO'] = df['RETORNO_novo'].where(
    df['RETORNO_novo'].notnull(), df['RETORNO'])
df.drop('RETORNO_novo', axis=1, inplace=True)

# Faça a segunda fusão
df = pd.merge(df, ret_melt_fundo, on=[
              'DT_COMPTC', 'CNPJ_FUNDO'], how='left', suffixes=('', '_novo'))

# Substitua os dados originais e exclua a coluna nova
df['RETORNO_PEER'] = df['RETORNO_PEER_novo'].where(
    df['RETORNO_PEER_novo'].notnull(), df['RETORNO_PEER'])
df.drop('RETORNO_PEER_novo', axis=1, inplace=True)

# Substituindo os valores existentes na coluna CONTRIBUICAO
df['CONTRIBUICAO'] = df['PESO'] * df['RETORNO']

# Pega a data máxima diretamente do banco de dados
query_max_data = 'SELECT MAX(DT_COMPTC) as max_data FROM cvm_peers_fia'
result = pd.read_sql(query_max_data, con=engine)
data_maxima = result['max_data'][0]  # Isso deve dar a data máxima

# Filtra o DataFrame para incluir apenas as datas maiores que a data máxima
df_filtrado = df[df['DT_COMPTC'] > data_maxima]


# Insere os dados do DataFrame no banco de dados na tabela 'nome_da_tabela'
# 'replace' irá substituir a tabela se ela já existir. Se preferir adicionar as linhas à tabela existente, use 'append'.
df_filtrado.to_sql('cvm_peers_fia_estimado', engine, if_exists='replace')

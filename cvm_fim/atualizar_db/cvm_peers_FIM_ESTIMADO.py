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


def ajustar_data(data, mes_vigente=11):
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

query = "SELECT * FROM cvm_peers_fim WHERE Gestor NOT LIKE 'Etrnty'"
df = pd.read_sql(query, con=engine)


# Adicione esta linha logo após ler o DataFrame df
colunas_originais = df.columns.tolist()

calendario = Brazil()
ano_atual = 2024
datas_desejadas = []

for mes in range(1, 4):  # Loop de janeiro (1) a março (3)
    if mes > 12:  # Ajuste para janeiro do ano seguinte, se necessário
        ano = ano_atual + 1
        mes_ajustado = 1
    else:
        ano = ano_atual
        mes_ajustado = mes
    
    # Calcula o último dia do mês
    data = pd.Timestamp(datetime(ano, mes_ajustado, 1))
    ultimo_dia_util = data + BMonthEnd(1)
    
    # Verificação específica para março por conta do feriado de Páscoa
    if mes == 3:
        # Define manualmente o dia 28 como último dia útil de março se necessário
        sexta_feira_santa = pd.Timestamp(datetime(ano, 3, 29))  # Sexta-feira Santa
        if ultimo_dia_util >= sexta_feira_santa:
            ultimo_dia_util = sexta_feira_santa - pd.Timedelta(days=1)
    
    datas_desejadas.append(ultimo_dia_util)

# Imprimindo as datas desejadas para verificar
for data in datas_desejadas:
    print(data.strftime('%Y-%m-%d'))

# Verifique os prints para confirmar se as datas estão sendo calculadas corretamente

novas_linhas = []
ultimas_linhas = df[df['DT_COMPTC'] == df['DT_COMPTC'].max()]

# Suponha que já tenhamos calculado datas_desejadas
# e tenhamos a data da Sexta-feira Santa para o ano em questão

ano_atual = 2024  # Definindo o ano atual para usar no ajuste da Sexta-feira Santa
sexta_feira_santa = pd.Timestamp(datetime(ano_atual, 3, 29))  # Sexta-feira Santa

for _, linha in ultimas_linhas.iterrows():
    for data in datas_desejadas:
        if data > df['DT_COMPTC'].max():
            # Cria uma cópia da linha para ajustar a data de competência
            nova_linha = linha.copy()
            nova_linha['DT_COMPTC'] = data

            # Se a data for a Sexta-feira Santa, ajuste para o dia útil anterior (28 de março)
            if nova_linha['DT_COMPTC'] == sexta_feira_santa:
                nova_linha['DT_COMPTC'] = sexta_feira_santa - pd.Timedelta(days=1)

            novas_linhas.append(nova_linha)

df = pd.concat([df, pd.DataFrame(novas_linhas)], ignore_index=True)

# Neste ponto, não é necessário adicionar BMonthEnd(0) a 'DT_COMPTC',
# pois já ajustamos para o último dia útil conforme necessário


fundos = pd.concat([df["CNPJ_FUNDO"], df["CNPJ_FUNDO_COTA"]]).unique().tolist()
start_date = datetime.strptime("06-30-2022", "%m-%d-%Y")
end_date = datetime.strptime("04-01-2024", "%m-%d-%Y")
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
query_max_data = 'SELECT MAX(DT_COMPTC) as max_data FROM cvm_peers_fim'
result = pd.read_sql(query_max_data, con=engine)
data_maxima = result['max_data'][0]  # Isso deve dar a data máxima

# Filtra o DataFrame para incluir apenas as datas maiores que a data máxima
df_filtrado = df[df['DT_COMPTC'] > data_maxima]


# Insere os dados do DataFrame no banco de dados na tabela 'nome_da_tabela'
# 'replace' irá substituir a tabela se ela já existir. Se preferir adicionar as linhas à tabela existente, use 'append'.
df_filtrado.to_sql('cvm_peers_fim_estimado', engine, if_exists='replace')

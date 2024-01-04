from datetime import datetime
from et_lib.ET_Meu_portfolio import Meu_portfolio_connection
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, Date, Text
import pandas as pd
from et_lib.ET_Data_Reader import QuantumHistoricalData
from pandas.tseries.offsets import MonthEnd


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

query_nm_fundo = 'SELECT CNPJ_FUNDO_COTA, NM_FUNDO_COTA FROM cvm_peers_fia'
df_nm_fundo = pd.read_sql(query_nm_fundo, con=engine)
mapping_dict = dict(
    zip(df_nm_fundo['CNPJ_FUNDO_COTA'], df_nm_fundo['NM_FUNDO_COTA']))


fund_name = "ETRNTY EVO FIC FIM"
ETR_CNPJ = "47.700.200/0001-10"
# fund_name="ETRNTY EVO FIC FIM"

# Busca todas as datas únicas da tabela
query_dates = 'SELECT DISTINCT DT_COMPTC FROM cvm_peers_fia_estimado'
df_dates = pd.read_sql(query_dates, con=engine)

# Converte para datetime (se já não for)
df_dates['DT_COMPTC'] = pd.to_datetime(df_dates['DT_COMPTC'])

dfs_to_concat = []

# Itera sobre as datas únicas e roda o seu código
for date in df_dates['DT_COMPTC']:
    dt = date

    # estabelece conexão
    meu_portfolio_connection = Meu_portfolio_connection(False)
    # pega data mais recente da carteira
    # dt=meu_portfolio_connection.get_portfolio_last_date(fund_name)
    my_fund = meu_portfolio_connection.get_portfolio_positions_as_df(
        fund_name, dt)

    # Primeiro, uma cópia do DataFrame original, para não perder informações.
    transformed_fund = my_fund.copy()

    # Adicione novas colunas ou renomeie conforme necessário.
    # Por exemplo, se 'ID' é o mesmo que 'CNPJ_FUNDO_COTA', você pode renomear essa coluna.
    transformed_fund.rename(columns={'ID': 'CNPJ_FUNDO_COTA'}, inplace=True)

    # Calcule a soma total da coluna 'Valor'
    total_valor = transformed_fund['Valor'].sum()

    # Filtra o DataFrame para manter apenas as linhas em que 'CNPJ_FUNDO_COTA' começa com um número
    transformed_fund = transformed_fund[transformed_fund['CNPJ_FUNDO_COTA'].str.startswith(
        tuple('0123456789'))]

    # Se você precisa adicionar colunas adicionais, você pode fazê-lo assim:
    # Aqui estou assumindo que você vai preencher esses valores posteriormente.
    transformed_fund['Gestor'] = 'Etrnty'
    transformed_fund['DT_COMPTC'] = dt
    transformed_fund['CNPJ_FUNDO'] = [ETR_CNPJ for _ in transformed_fund.index]
    transformed_fund['NM_FUNDO_COTA'] = transformed_fund['CNPJ_FUNDO_COTA'].map(
        mapping_dict)
    transformed_fund['PESO'] = transformed_fund['Valor'] / total_valor

    # Aplicar lógica específica para um CNPJ em particular
    transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '51.152.458/0001-05',
                         'NM_FUNDO_COTA'] = '3 ILHAS FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'
    transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '26.243.348/0001-01',
                         'NM_FUNDO_COTA'] = 'IBIUNA EQUITIES 30 FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'
    transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '37.887.412/0001-03',
                         'NM_FUNDO_COTA'] = 'SHARP EQUITY VALUE ADVISORY FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'

    # Agora você pode selecionar apenas as colunas que você quer.
    final_columns = ['Gestor', 'DT_COMPTC', 'CNPJ_FUNDO',
                     'CNPJ_FUNDO_COTA', 'NM_FUNDO_COTA', 'PESO']
    transformed_fund = transformed_fund[final_columns]

    fundos = pd.concat(
        [transformed_fund["CNPJ_FUNDO"], transformed_fund["CNPJ_FUNDO_COTA"]]).unique().tolist()
    start_date = datetime.strptime("06-30-2023", "%m-%d-%Y")
    end_date = datetime.strptime("02-01-2024", "%m-%d-%Y")
    q = QuantumHistoricalData(start_date, end_date, fundos, [
        "PX_LAST"], "MONTHLY")

    precos = q.getData()
    precos = precos.droplevel(1, axis=1)

    ret = precos / precos.shift(1) - 1

    # Resetando o índice para transformar a data em uma coluna
    ret.reset_index(inplace=True)
    ret.rename(columns={'index': 'DT_COMPTC'}, inplace=True)

    # Derretendo o DataFrame para CNPJ_FUNDO_COTA
    ret_melt_cota = ret.melt(
        id_vars=['DT_COMPTC'], var_name='CNPJ_FUNDO_COTA', value_name='RETORNO')

    # Derretendo o DataFrame para CNPJ_FUNDO
    ret_melt_fundo = ret.melt(
        id_vars=['DT_COMPTC'], var_name='CNPJ_FUNDO', value_name='RETORNO_PEER')

    # Convertendo 'DT_COMPTC' para o mesmo tipo de datetime em todos os DataFrames
    ret_melt_cota['DT_COMPTC'] = pd.to_datetime(ret_melt_cota['DT_COMPTC'])
    ret_melt_fundo['DT_COMPTC'] = pd.to_datetime(ret_melt_fundo['DT_COMPTC'])
    transformed_fund['DT_COMPTC'] = pd.to_datetime(
        transformed_fund['DT_COMPTC'])

    # Atualizando 'DT_COMPTC' para o último dia do mês
    ret_melt_cota['DT_COMPTC'] += MonthEnd(0)
    ret_melt_fundo['DT_COMPTC'] += MonthEnd(0)
    transformed_fund['DT_COMPTC'] += MonthEnd(0)

    # Primeiro merge com CNPJ_FUNDO_COTA
    final_result = pd.merge(transformed_fund, ret_melt_cota, how='left', on=[
        'DT_COMPTC', 'CNPJ_FUNDO_COTA'])

    # Segundo merge com CNPJ_FUNDO
    final_result = pd.merge(final_result, ret_melt_fundo,
                            how='left', on=['DT_COMPTC', 'CNPJ_FUNDO'])

    # Calculando o CONTRIBUICAO
    final_result['CONTRIBUICAO'] = final_result['PESO'] * \
        final_result['RETORNO']
    dfs_to_concat.append(final_result)

final_df = pd.concat(dfs_to_concat, ignore_index=True)
final_df.to_excel("avaliacao_estrutura_dados.xlsx", index=False)
# Salvando em SQL
final_df.to_sql('cvm_peers_fia_estimado', con=engine,
                if_exists='append', index=False)

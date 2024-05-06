from typing import Literal
from datetime import datetime
from et_lib.ET_Meu_portfolio import Meu_portfolio_connection
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, Date, Text
import pandas as pd
from gb import Carteira
from et_lib.ET_Data_Reader import QuantumHistoricalData
from et_lib.ettools import str_to_cnpj
from pandas.tseries.offsets import MonthEnd

def get_fof_britech(fundo:Literal["ETRNTY EON MM MASTER FIC FIM"]|Literal["ETRNTY EVO FIC FIM"], data_pos:datetime=datetime.today())->pd.DataFrame:
    if fundo.upper()=="ETRNTY EON MM MASTER FIC FIM":
        conta="685038"
        cod_etrnty="ÉON"
    elif fundo.upper()=="ETRNTY EVO FIC FIM":
        conta="684627"
        cod_etrnty="EVO"
    else:
        return pd.DataFrame()
    df= Carteira.get_posicao_carteira(ids_carteira=conta,date_pos=data_pos,cod_etr=cod_etrnty)
    df=df.loc[df["Blotter"]==False]
    df=df.loc[df["DescricaoTipoPosicao"]=="Fundo"]
    df=df[["CNPJ","ValorBruto","QtdeTotal"]]
    df.loc[:,"CNPJ"]=df["CNPJ"].apply(lambda x: str_to_cnpj(x))
    df=df.rename(columns={'CNPJ': 'CNPJ_FUNDO_COTA',"ValorBruto":"Valor","QtdeTotal":"Qtt"})
    total_valor = df['Valor'].sum()

    # Filtra o DataFrame para manter apenas as linhas em que 'CNPJ_FUNDO_COTA' começa com um número
    df = df[df['CNPJ_FUNDO_COTA'].str.startswith(tuple('0123456789B'))]

    # Se você precisa adicionar colunas adicionais, você pode fazê-lo assim:
    # Aqui estou assumindo que você vai preencher esses valores posteriormente.
    df['Gestor'] = 'Etrnty'
    df['DT_COMPTC'] = dt
    df['CNPJ_FUNDO'] = [ETR_CNPJ for _ in df.index]
    df['NM_FUNDO_COTA'] = df['CNPJ_FUNDO_COTA'].map(mapping_dict)
    df['PESO'] = df['Valor'] / total_valor
    df=df[['Gestor', 'DT_COMPTC', 'CNPJ_FUNDO','CNPJ_FUNDO_COTA', 'NM_FUNDO_COTA', 'PESO']]
    return df


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

params=[
    {"query_table":"cvm_peers_fim","query_table_estimado":"cvm_peers_fim_estimado","fund_name":"ETRNTY EON MM MASTER FIC FIM","fof_cnpj":"47.716.356/0001-90"},
    {"query_table":"cvm_peers_fia", "query_table":"cvm_peers_fia_estimado", "fund_name":"ETRNTY EVO FIC FIM","fof_cnpj":"47.700.200/0001-10" }]

for param in params:
    query_nm_fundo = 'SELECT CNPJ_FUNDO_COTA, NM_FUNDO_COTA FROM '+param["query_table"]
    df_nm_fundo = pd.read_sql(query_nm_fundo, con=engine)
    mapping_dict = dict(
        zip(df_nm_fundo['CNPJ_FUNDO_COTA'], df_nm_fundo['NM_FUNDO_COTA']))


    fund_name = param["fund_name"]
    ETR_CNPJ = param["fof_cnpj"]

    # Busca todas as datas únicas da tabela
    query_dates = 'SELECT DISTINCT DT_COMPTC FROM '+param["query_table_estimado"]
    df_dates = pd.read_sql(query_dates, con=engine)
    print("df_dates - carteira_etr_sql.py linha 75:")
    print(df_dates)

    # Converte para datetime (se já não for)
    df_dates['DT_COMPTC'] = pd.to_datetime(df_dates['DT_COMPTC'])

    dfs_to_concat = []

    for date in df_dates['DT_COMPTC']:
        dt = date
        print(param["fund_name"]+" "+dt.strftime("%Y-%m-%d"))

        my_fund=get_fof_britech(fund_name,dt)   #type:ignore
        if my_fund.empty==True:
            raise ValueError("Não foi possível encontrar a carteira para a data "+dt.strftime("%Y-%m-%d"))
        # Primeiro, uma cópia do DataFrame original, para não perder informações.
        transformed_fund = my_fund.copy()

            # Aplicar lógica específica para um CNPJ em particular
        #transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '51.152.458/0001-05',
        #                     'NM_FUNDO_COTA'] = '3 ILHAS FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'
        #transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '26.243.348/0001-01',
        #                     'NM_FUNDO_COTA'] = 'IBIUNA EQUITIES 30 FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'
        #transformed_fund.loc[transformed_fund['CNPJ_FUNDO_COTA'] == '37.887.412/0001-03',
        #                     'NM_FUNDO_COTA'] = 'SHARP EQUITY VALUE ADVISORY FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO EM AÇÕES'


        fundos = pd.concat([transformed_fund["CNPJ_FUNDO"], transformed_fund["CNPJ_FUNDO_COTA"]]).unique().tolist()
        start_date = datetime.strptime("06-30-2023", "%m-%d-%Y")
        end_date = datetime.strptime("02-01-2024", "%m-%d-%Y")
        q = QuantumHistoricalData(start_date, end_date, fundos, ["PX_LAST"], "MONTHLY")

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
    #final_df.to_excel("avaliacao_estrutura_dados.xlsx", index=False)
    # Salvando em SQL
    final_df.to_sql(param["query_table_estimado"], con=engine,
                    if_exists='append', index=False)
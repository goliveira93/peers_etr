"""
Le arquvios com posições CVM dos fundos dos concorrentes e compara sua 
composição com a do nosso fundo (baixado do meuportfolio)
"""

from datetime import datetime
import os
import pandas as pd
import plotly.graph_objects as go
from et_lib.ET_Meu_portfolio import Meu_portfolio_connection
from et_lib.ET_Data_Reader import QuantumHistoricalData
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, Date, Text
from pandas.tseries.offsets import MonthEnd
from dotenv import load_dotenv

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

metadata = MetaData()

# Define a tabela no MySQL
my_table = Table('cvm_peers_fia', metadata,
                 Column('Gestor', String(18)),
                 Column('DT_COMPTC', Date),
                 Column('CNPJ_FUNDO', String(18)),
                 Column('CNPJ_FUNDO_COTA', String(18)),
                 Column('NM_FUNDO_COTA', Text),
                 Column('PESO', Float),
                 Column('RETORNO', Float),
                 Column('CONTRIBUICAO', Float),
                 Column('RETORNO_PEER', Float)
                 )


FIAS = [["26.470.647/0001-70", "Mandatto"],
        ["13.958.690/0001-38", "Taler"],
        ["07.875.686/0001-03", "Consenso"],
        ["37.703.536/0001-83", "Warren"],
        ["34.633.416/0001-69", "Vitra"],
        ["06.128.183/0001-01", "JBFO"],
        ["24.371.991/0001-87", "Wright"],
        ["11.389.643/0001-95", "Pragma"],
        ["35.610.530/0001-36", "Brain"],
        ["18.060.935/0001-29", "G5"],
        ["11.541.999/0001-00", "São João"],
        ["25.098.042/0001-38", "XPA"],
        ["37.227.781/0001-61", "Portofino"],
        ["47.700.200/0001-10", "Etrnty"],
        ["14.096.759/0001-24", "FoF Itau"],
        ["41.575.654/0001-20", "We Capital"]
        ]




new_feeders = [
    ["51.162.466/0001-24", "13.962.959/0001-50", "Ibiúna Long Short"]
]

config = {"EVO": {"ETR_CNPJ": "47.700.200/0001-10", "meu_portfolio": "ETRNTY EVO FIC FIM", "PEERS": FIAS, "filename": "EVO"}}




if __name__ == "__main__":
    file = "cda_fi_BLC_2_202406.csv"  # alterar o nome do arquivo para o mais recente
    pl_file = "cda_fi_PL_202406.csv"  # alterar o nome do arquivo para o mais recente
    file_path=os.path.join(".", "data")
    conf = config["EVO"]

    pl = pd.read_csv(os.path.join(file_path,pl_file), delimiter=";", encoding="ISO-8859-1")
    # ETR = read_my_portfolio(conf["meu_portfolio"], pl, conf["ETR_CNPJ"])
    pl = pl[["CNPJ_FUNDO", "VL_PATRIM_LIQ"]]
    pl = pl.set_index("CNPJ_FUNDO")
    # pl.loc[conf["ETR_CNPJ"]] = ETR["VL_MERC_POS_FINAL"].sum()

    df = pd.read_csv(os.path.join(file_path,file), delimiter=";", encoding="ISO-8859-1", low_memory=False)
    df = df[["TP_FUNDO", "DT_COMPTC", "CNPJ_FUNDO", "DENOM_SOCIAL", "TP_ATIVO",
             "VL_MERC_POS_FINAL", "CNPJ_FUNDO_COTA", "NM_FUNDO_COTA"]]
    result = pd.DataFrame()

    for f in conf["PEERS"]:  # mudar numero
        fundo = f[0]
        sub_df = df.loc[df["CNPJ_FUNDO"] == fundo].copy()
        if sub_df.empty is True:
            continue

        sub_df["PESO"] = sub_df["VL_MERC_POS_FINAL"] / \
            float(pl.loc[fundo, "VL_PATRIM_LIQ"])  # type: ignore
        sub_df["Gestor"] = [f[1] for _ in sub_df.index]

        result = pd.concat([result, sub_df], axis=0)

    result.reset_index(drop=True, inplace=True)
    result = result[["Gestor", "DT_COMPTC", "CNPJ_FUNDO",
                     "CNPJ_FUNDO_COTA", "NM_FUNDO_COTA", "PESO"]]
    # print(result)
    # result = result.loc[result["Gestor"] == "Mandatto"]  # MUDAR
    fundos = pd.concat(
        [result["CNPJ_FUNDO"], result["CNPJ_FUNDO_COTA"]]).unique().tolist()
    start_date = datetime.strptime("01-29-2022", "%m-%d-%Y")
    end_date = datetime.strptime("12-01-2024", "%m-%d-%Y")
    q = QuantumHistoricalData(start_date, end_date, fundos, [
                              "PX_LAST"], "MONTHLY")

    precos = q.getData()
    precos = precos.droplevel(1, axis=1)

    ret = precos / precos.shift(1) - 1

# Resetando o índice para transformar a data em uma coluna
    ret.reset_index(inplace=True)
    ret.rename(columns={'index': 'DT_COMPTC'}, inplace=True)

    # Derretendo o DataFrame para CNPJ_FUNDO_COTA
    ret_melt_cota = ret.melt(id_vars=['DT_COMPTC'],
                             var_name='CNPJ_FUNDO_COTA', value_name='RETORNO')

    # Derretendo o DataFrame para CNPJ_FUNDO
    ret_melt_fundo = ret.melt(id_vars=['DT_COMPTC'],
                              var_name='CNPJ_FUNDO', value_name='RETORNO_PEER')

    # Convertendo 'DT_COMPTC' para o mesmo tipo de datetime em todos os DataFrames
    ret_melt_cota['DT_COMPTC'] = pd.to_datetime(ret_melt_cota['DT_COMPTC'])
    ret_melt_fundo['DT_COMPTC'] = pd.to_datetime(ret_melt_fundo['DT_COMPTC'])
    result['DT_COMPTC'] = pd.to_datetime(result['DT_COMPTC'])

    # Atualizando 'DT_COMPTC' para o último dia do mês
    ret_melt_cota['DT_COMPTC'] += MonthEnd(0)
    ret_melt_fundo['DT_COMPTC'] += MonthEnd(0)
    result['DT_COMPTC'] += MonthEnd(0)

    # Primeiro merge com CNPJ_FUNDO_COTA
    final_result = pd.merge(result, ret_melt_cota, how='left', on=[
                            'DT_COMPTC', 'CNPJ_FUNDO_COTA'])

    # Segundo merge com CNPJ_FUNDO
    final_result = pd.merge(final_result, ret_melt_fundo,
                            how='left', on=['DT_COMPTC', 'CNPJ_FUNDO'])

    # Calculando o CONTRIBUICAO
    final_result['CONTRIBUICAO'] = final_result['PESO'] * \
        final_result['RETORNO']

    # Salvando em SQL
    final_result.to_sql('cvm_peers_fia', con=engine,
                        if_exists='append', index=False)

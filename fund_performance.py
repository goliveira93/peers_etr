import pandas as pd
from datetime import datetime
from et_lib.ET_Data_Reader import QuantumHistoricalData
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from libs.sinonimos import dicionario_sinonimos
import plotly.graph_objects as go
from libs.db_functions import get_fund_return


diretorio_base = os.path.join(".", "figures")

colors=["#2C4257",
        "#48728A",
        "#708F92",
        "#A3ABA4",
        "#605869",   #cor principal  para texto de corpo
        "#948794",
        "#E7A75F",   #Apenas para detalhes em elementos gráficos
        "#A25B1E"    #Apenas em gráficos
        ]

chart_layout = dict(
    height=820,  # Altura do gráfico
    width=2000,
    font={"family":"Segoe UI","size":12},
    # legend={"orientation":"h"},
    xaxis= {"tickformat":",","showgrid":False, "zeroline":False},
    yaxis= {"tickformat":".2s","showgrid":False, "zeroline":False},
    margin=dict(l=20, r=20, t=25, b=25),
    hovermode = "x",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém as variáveis de ambiente
user = os.getenv("user")
password = os.getenv("password")
host = os.getenv("host")
port = os.getenv("port")
database = os.getenv("database")

# Conexão com o banco de dados
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")

tables = {"EON":["cvm_peers_fim_estimado",["IFMM BTG Pactual","CDI"], "FIM"], "EVO":["cvm_peers_fia_estimado",["IBX"], "FIA"]}


def calcula_retorno_mensal_benchmark(benchmark: str, start_date: datetime, end_date: datetime):
    # Verifica se o benchmark é válido
    if benchmark not in ["IBX", "IFMM BTG Pactual", "CDI"]:
        raise ValueError("Benchmark inválido.")

    # Obtém os dados históricos usando o Quantum
    q = QuantumHistoricalData(start_date, end_date, [benchmark], ["PX_LAST"], "MONTHLY")
    precos = q.getData()
    precos = precos.droplevel(1, axis=1)

    # Calcula os retornos mensais
    retornos = precos.pct_change().iloc[-1]  # Seleciona apenas o último retorno (no dia 31-01-2024)
    retornos = retornos.reset_index()
    retornos.columns = ['NM_FUNDO_COTA', 'RETORNO_MENSAL']

    return retornos

def calcula_retorno_ytd_benchmark(benchmark: str, start_date: datetime, end_date: datetime):
    # Verifica se o benchmark é válido
    if benchmark not in ["IBX", "IFMM BTG Pactual", "CDI"]:
        raise ValueError("Benchmark inválido.")
    start_date = datetime(2023, 12, 29)

    # Obtém os dados históricos usando o Quantum
    q = QuantumHistoricalData(start_date, end_date, [benchmark], ["PX_LAST"], "DAILY")
    precos = q.getData()
    precos = precos.droplevel(1, axis=1)

    # Calcula os retornos diários
    retornos_diarios = precos.pct_change()

    # Calcula o retorno acumulado YTD usando cumprod
    retorno_acumulado = (1 + retornos_diarios).cumprod() - 1

    # Seleciona apenas o retorno acumulado no último dia (29-02-2024)
    retorno_acumulado = retorno_acumulado.iloc[-1]

    # Cria um DataFrame com os retornos acumulados
    retornos = pd.DataFrame({
        'NM_FUNDO_COTA': retorno_acumulado.index,
        'RETORNO_YTD': retorno_acumulado.values
    })

    return retornos


def gera_df_performance(fundo: str, save_files: bool = True) -> pd.DataFrame:
    diretorio_base = os.path.join(".", "figures")
    if not os.path.exists(diretorio_base):
        os.makedirs(diretorio_base)
    #####  Configuração #########
    table = tables[fundo]
    # Consulta para buscar dados específicos de '2024-01-31'

    # Definição dos benchmarks com base no tipo de fundo
    if fundo == "EON":
        benchmark_names = ["CDI", "IFMM BTG Pactual"]
    elif fundo == "EVO":
        benchmark_names = ["IBX"]
    else:
        raise ValueError("Tipo de fundo não reconhecido.")

    query = "SELECT DT_COMPTC, CNPJ_FUNDO_COTA, NM_FUNDO_COTA FROM "+ table[0] +" WHERE Gestor NOT IN ('FoF Itaú', 'São João') AND DT_COMPTC >= '2024-01-31 00:00:00'"
    df = pd.read_sql(query, con=engine)

    query2 = "SELECT Gestor, DT_COMPTC, NM_FUNDO_COTA FROM " + table[0] + " WHERE Gestor NOT IN ('FoF Itaú', 'São João') AND DT_COMPTC >= '2024-01-31 00:00:00'"
    df2 = pd.read_sql(query2, con=engine)

    # Concatenando CNPJs de fundos e fundos cota para criar uma série única
    df['NM_FUNDO_COTA'].replace(dicionario_sinonimos, inplace=True)
    df['NM_FUNDO_COTA'].drop_duplicates()
    df2['NM_FUNDO_COTA'].replace(dicionario_sinonimos, inplace=True)

    # Adicionando linhas adicionais com base no valor de "table"
    for benchmark in table[1]:
        nova_linha = {"DT_COMPTC": df2.iloc[1]["DT_COMPTC"], "NM_FUNDO_COTA": benchmark, "CNPJ_FUNDO_COTA":benchmark}
        df = df._append(nova_linha, ignore_index=True)
        nova_linha2 = {"Gestor": "B3", "DT_COMPTC": df2.iloc[1]["DT_COMPTC"], "NM_FUNDO_COTA": benchmark}
        df2 = df2._append(nova_linha2, ignore_index=True)

        fundos_series = df["NM_FUNDO_COTA"].drop_duplicates()


    # Data de início e fim para o cálculo do retorno
    start_date = datetime(2024,1,31)
    end_date = datetime(2024,2,29)

    fundos_series = df["NM_FUNDO_COTA"].drop_duplicates()

    # Inicialize uma lista vazia para armazenar os retornos mensais
    retornos_mensais = []
    retornos_ytd = []  # Lista para armazenar os retornos YTD

    # Itera sobre os nomes dos fundos em fundos_series
    for fundo in fundos_series:
        # Chama a função get_fund_return para obter o retorno mensal para o fundo atual
        retorno_mensal = get_fund_return(fundo, end_date, end_date)
        # Adiciona o retorno mensal à lista se for diferente de NaN e não estiver vazio
        if not pd.isnull(retorno_mensal) and retorno_mensal != 0:
            retornos_mensais.append({"NM_FUNDO_COTA": fundo, "RETORNO_MENSAL": retorno_mensal})

        # Calcular o retorno YTD
        retorno_ytd = get_fund_return(fundo, start_date, end_date)
        if not pd.isnull(retorno_ytd) and retorno_ytd != 0:
            retornos_ytd.append({"NM_FUNDO_COTA": fundo, "RETORNO_YTD": retorno_ytd})

    # Converte as listas de retornos mensais e YTD em DataFrames
    df_retornos_mensais = pd.DataFrame(retornos_mensais)
    df_retornos_ytd = pd.DataFrame(retornos_ytd)

    # Adicionando os benchmarks aos retornos mensais se não estiverem presentes
    for benchmark in benchmark_names:
        if benchmark not in df_retornos_mensais['NM_FUNDO_COTA'].values:
            retorno_mensal_df = calcula_retorno_mensal_benchmark(benchmark, start_date, end_date)
            retorno_mensal = retorno_mensal_df.loc[0, 'RETORNO_MENSAL']
            novo_registro = {"NM_FUNDO_COTA": benchmark, "RETORNO_MENSAL": retorno_mensal}
            df_retornos_mensais = df_retornos_mensais._append(novo_registro, ignore_index=True)

        if benchmark not in df_retornos_ytd['NM_FUNDO_COTA'].values:
            retorno_ytd_df = calcula_retorno_ytd_benchmark(benchmark, start_date, end_date)
            retorno_ytd = retorno_ytd_df.loc[0, 'RETORNO_YTD']
            novo_registro = {"NM_FUNDO_COTA": benchmark, "RETORNO_YTD": retorno_ytd}
            df_retornos_ytd = df_retornos_ytd._append(novo_registro, ignore_index=True)

    # Removendo os retornos que são NaN
    df_retornos_mensais = df_retornos_mensais.dropna(subset=['RETORNO_MENSAL'])
    df_retornos_mensais = df_retornos_mensais.sort_values(by='RETORNO_MENSAL', ascending=False)

    df_retornos_ytd = df_retornos_ytd.dropna(subset=['RETORNO_YTD'])
    df_retornos_ytd = df_retornos_ytd.sort_values(by='RETORNO_YTD', ascending=False)

    if save_files is True:
        # Gráfico MTD
        fig_mtd = go.Figure(go.Bar(
            x=df_retornos_mensais['NM_FUNDO_COTA'],  # Nomes dos fundos
            y=df_retornos_mensais['RETORNO_MENSAL'],  # Retornos mensais médios
            text=df_retornos_mensais['RETORNO_MENSAL'].apply(lambda x: f"<b>{x:.2%}</b>"),  # Texto formatado em negrito
            hoverinfo='text',  # Informações ao passar o mouse
            marker_color=[
                colors[1] if fundo in df2.loc[df2['Gestor'] == 'Etrnty', 'NM_FUNDO_COTA'].values 
                else colors[7] if fundo in df2.loc[df2['Gestor'] == 'B3', 'NM_FUNDO_COTA'].values 
                else colors[3]# ou qualquer outra cor que você deseje
                for fundo in df_retornos_mensais['NM_FUNDO_COTA']
            ]))

        # Atualizando o layout do gráfico MTD
        fig_mtd.update_layout(chart_layout)
        fig_mtd.update_layout(
            font=dict(family='Segoe UI', size=14),  # Estilo da fonte
            bargap=0.1,  # Espaçamento entre as barras
            xaxis={
                "tickformat": ",", 
                "showgrid": False, 
                "zeroline": False,
                "tickangle": 90,  # Rótulos na vertical
                "automargin": True
            },
            yaxis={
                "tickformat": ".1%", 
                "showgrid": False, 
                "zeroline": False
            },
            margin=dict(l=20, r=20, t=20, b=200), # Ajuste a margem inferior conforme necessário para acomodar os rótulos
            hovermode="y",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Exibindo e salvando o gráfico MTD
        fig_mtd.write_image(os.path.join(diretorio_base, f"{table[0]}_retorno_mtd.png"))
        # fig_mtd.show()
        print("Gerando retornos MTD")

        # Gráfico YTD
        fig_ytd = go.Figure(go.Bar(
            x=df_retornos_ytd['NM_FUNDO_COTA'],  # Nomes dos fundos
            y=df_retornos_ytd['RETORNO_YTD'],  # Retornos YTD
            text=df_retornos_ytd['RETORNO_YTD'].apply(lambda x: f"<b>{x:.2%}</b>"),  # Texto formatado em negrito
            hoverinfo='text',  # Informações ao passar o mouse
            marker_color=[
                colors[1] if fundo in df2.loc[df2['Gestor'] == 'Etrnty', 'NM_FUNDO_COTA'].values 
                else colors[7] if fundo in df2.loc[df2['Gestor'] == 'B3', 'NM_FUNDO_COTA'].values 
                else colors[3]# ou qualquer outra cor que você deseje
                for fundo in df_retornos_ytd['NM_FUNDO_COTA']
            ]))

        # Atualizando o layout do gráfico YTD
        fig_ytd.update_layout(chart_layout)
        fig_ytd.update_layout(
            font=dict(family='Segoe UI', size=14),  # Estilo da fonte
            bargap=0.1,  # Espaçamento entre as barras
            xaxis={
                "tickformat": ",", 
                "showgrid": False, 
                "zeroline": False,
                "tickangle": 90,  # Rótulos na vertical
                "automargin": True
            },
            yaxis={
                "tickformat": ".1%", 
                "showgrid": False, 
                "zeroline": False
            },
            margin=dict(l=20, r=20, t=20, b=200), # Ajuste a margem inferior conforme necessário para acomodar os rótulos
            hovermode="y",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )

        # Exibindo e salvando o gráfico YTD
        fig_ytd.write_image(os.path.join(diretorio_base, f"{table[0]}_retorno_ytd.png"))
        # fig_ytd.show()
        print("Gerando retornos YTD")

    return df_retornos_mensais, df_retornos_ytd

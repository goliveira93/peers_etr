import pandas as pd
from datetime import datetime
from et_lib.ET_Data_Reader import QuantumHistoricalData
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from libs.sinonimos import dicionario_sinonimos
import plotly.graph_objects as go


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

# Consulta para buscar dados específicos de '2024-01-31'
query = "SELECT DT_COMPTC, CNPJ_FUNDO_COTA, NM_FUNDO_COTA FROM cvm_peers_fia_estimado WHERE DT_COMPTC = '2024-01-31 00:00:00' limit 10"
df = pd.read_sql(query, con=engine)

query2 = "SELECT Gestor, DT_COMPTC, NM_FUNDO_COTA FROM cvm_peers_fia_estimado WHERE DT_COMPTC = '2024-01-31 00:00:00' limit 10"
df2 =pd.read_sql(query2, con=engine)

# Concatenando CNPJs de fundos e fundos cota para criar uma série única
df['NM_FUNDO_COTA'].replace(dicionario_sinonimos, inplace=True)
df['NM_FUNDO_COTA'].drop_duplicates()
nova_linha1 = {"DT_COMPTC": df2.iloc[1]["DT_COMPTC"], "NM_FUNDO_COTA": "IBX", "CNPJ_FUNDO_COTA":"IBX"}
df = df._append(nova_linha1, ignore_index=True)

df2['NM_FUNDO_COTA'].replace(dicionario_sinonimos, inplace=True)
df2['NM_FUNDO_COTA'].drop_duplicates()
nova_linha2 = {"Gestor": "B3", "DT_COMPTC": df2.iloc[1]["DT_COMPTC"], "NM_FUNDO_COTA": "IBX"}
df2 = df2._append(nova_linha2, ignore_index=True)

# Função para calcular retorno mensal ajustado
def calcula_retorno_mensal(fundos: pd.Series, start_date: datetime, end_date: datetime):
    fds = fundos.unique().tolist()
    q = QuantumHistoricalData(start_date, end_date, fds, ["PX_LAST"], "MONTHLY")
    precos = q.getData()
    precos = precos.droplevel(1, axis=1)

    retornos = precos.pct_change().iloc[-1]  # Seleciona apenas o último retorno (no dia 31-01-2024)
    retornos = retornos.reset_index()
    retornos.columns = ['CNPJ_FUNDO_COTA', 'RETORNO_MENSAL']
    retornos['DT_COMPTC'] = end_date  # Define a data de competência como sendo o último dia do período

    return retornos

# Função para calcular retorno anual ajustado
def calcula_retorno_anual(fundos: pd.Series, start_date: datetime, end_date: datetime):
    fds = fundos.unique().tolist()
    q_start = QuantumHistoricalData(start_date, start_date, fds, ["PX_LAST"], "DAILY")
    q_end = QuantumHistoricalData(end_date, end_date, fds, ["PX_LAST"], "DAILY")
    
    precos_start = q_start.getData()
    precos_end = q_end.getData()

    # Obtendo o preço do último dia
    precos_end = precos_end.droplevel(1, axis=1).iloc[-1]

    # Obtendo o preço do dia inicial
    precos_start = precos_start.droplevel(1, axis=1).iloc[-1]

    # Calculando o retorno anual ajustado
    retornos = precos_end / precos_start - 1

    # Convertendo para DataFrame
    retornos = pd.DataFrame(retornos, columns=['RETORNO_ANUAL'])

    # Adicionando as colunas CNPJ_FUNDO_COTA e DT_COMPTC
    retornos['CNPJ_FUNDO_COTA'] = retornos.index
    retornos['DT_COMPTC'] = end_date

    return retornos

# Data de início e fim para o cálculo do retorno
start_date = datetime.strptime("12-29-2023", "%m-%d-%Y")
end_date = datetime.strptime("02-05-2024", "%m-%d-%Y")



fundos_series = df["CNPJ_FUNDO_COTA"].drop_duplicates()

# Calculando os retornos mensais e anuais para os fundos
retornos_mensais = calcula_retorno_mensal(fundos_series, start_date, end_date)
retornos_anuais = calcula_retorno_anual(fundos_series, start_date, end_date)

# Merge dos retornos mensais e anuais com o dataframe original
df_final = pd.merge(df, retornos_mensais, on="CNPJ_FUNDO_COTA", how="left")
df_final = pd.merge(df_final, retornos_anuais, on="CNPJ_FUNDO_COTA", how="left")

# Removendo as colunas duplicadas DT_COMPTC_x e DT_COMPTC_y
df_final.drop(columns=['DT_COMPTC_x', 'DT_COMPTC_y'], inplace=True)

# Renomeando a coluna RETORNO_ANUAL para evitar conflito com DT_COMPTC
df_final.rename(columns={'RETORNO_ANUAL': 'RETORNO_ANUAL_PERCENTUAL'}, inplace=True)



# Filtrando os ativos que não possuem "caixa" no nome
df_final = df_final[~df_final['NM_FUNDO_COTA'].str.contains('caixa', case=False)]

# Removendo a coluna CNPJ_FUNDO_COTA antes de calcular os retornos
df_final.drop(columns=['CNPJ_FUNDO_COTA'], inplace=True)

# Agrupando por nome do fundo e calculando os retornos mensais para cada fundo
df_grouped = df_final.groupby('NM_FUNDO_COTA').mean().reset_index()
df_grouped = df_grouped.sort_values(by='RETORNO_MENSAL', ascending=False)
df_grouped_ytd = df_final.groupby('NM_FUNDO_COTA').mean().reset_index()
df_grouped_ytd = df_grouped.sort_values(by='RETORNO_ANUAL_PERCENTUAL', ascending=False)

# Criando o gráfico de barras
fig = go.Figure(go.Bar(
    x=df_grouped['NM_FUNDO_COTA'],  # Nomes dos fundos
    y=df_grouped['RETORNO_MENSAL'],  # Retornos mensais médios
    text=df_grouped['RETORNO_MENSAL'].apply(lambda x: f"<b>{x:.2%}</b>"),  # Texto formatado em negrito
    hoverinfo='text',  # Informações ao passar o mouse
    marker_color=[
        colors[1] if fundo in df2.loc[df2['Gestor'] == 'Etrnty', 'NM_FUNDO_COTA'].values 
        else colors[7] if fundo in df2.loc[df2['Gestor'] == 'B3', 'NM_FUNDO_COTA'].values 
        else colors[3]# ou qualquer outra cor que você deseje
        for fundo in df_grouped['NM_FUNDO_COTA']
]))

# Atualizando o layout do gráfico
fig.update_layout(chart_layout)
fig.update_layout(
    title='Retorno MTD',
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

# Exibindo o gráfico
fig.show()

# Criando o gráfico de barras
fig = go.Figure(go.Bar(
    x=df_grouped_ytd['NM_FUNDO_COTA'],  # Nomes dos fundos
    y=df_grouped_ytd['RETORNO_ANUAL_PERCENTUAL'],  # Retornos mensais médios
    text=df_grouped_ytd['RETORNO_ANUAL_PERCENTUAL'].apply(lambda x: f"<b>{x:.2%}</b>"),  # Texto formatado em negrito
    hoverinfo='text',  # Informações ao passar o mouse
    marker_color=[
        colors[1] if fundo in df2.loc[df2['Gestor'] == 'Etrnty', 'NM_FUNDO_COTA'].values 
        else colors[7] if fundo in df2.loc[df2['Gestor'] == 'B3', 'NM_FUNDO_COTA'].values 
        else colors[3]# ou qualquer outra cor que você deseje
        for fundo in df_grouped_ytd['NM_FUNDO_COTA']
]))

# Atualizando o layout do gráfico
fig.update_layout(chart_layout)
fig.update_layout(
    title='Retorno MTD',
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

# Exibindo o gráfico
fig.show()
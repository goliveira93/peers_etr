import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from workalendar.america import Brazil
from pandas.tseries.offsets import BMonthEnd
import json
from dotenv import load_dotenv
import os
import numpy as np
import plotly.graph_objects as go
from sinonimos import dicionario_sinonimos
# from plotly.subplots import make_subplots

diretorio_base = "C:\\Users\\Gustavo.Oliveira\\ETRNTY Capital Planejadora Financeira\\Investimentos - Documentos\\Performance mensal\\performance img\\fia"

colors = ["#2C4257",
          "#48728A",
          "#708F92",
          "#A3ABA4",
          "#605869",  # cor principal  para texto de corpo
          "#948794",
          "#E7A75F",  # Apenas para detalhes em elementos gráficos
          "#A25B1E"  # Apenas em gráficos
          ]


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

query = '''SELECT Gestor, DT_COMPTC, CNPJ_FUNDO, CNPJ_FUNDO_COTA, NM_FUNDO_COTA, PESO, RETORNO, RETORNO_PEER, CONTRIBUICAO 
           FROM cvm_peers_fia  '''

query2 = '''SELECT Gestor, DT_COMPTC, CNPJ_FUNDO, CNPJ_FUNDO_COTA, NM_FUNDO_COTA, PESO, RETORNO, RETORNO_PEER, CONTRIBUICAO 
            FROM cvm_peers_fia_estimado  '''

query_combinada = query + ' UNION ALL ' + query2


df = pd.read_sql(query_combinada, con=engine)

# Cria a nova coluna com os valores da coluna original
df['FUNDO_AJUSTADO'] = df['NM_FUNDO_COTA']

# Faz as substituições com base no dicionário de sinônimos
df['FUNDO_AJUSTADO'].replace(dicionario_sinonimos, inplace=True)

# Ajustes de colunas para o merge e cálculos
df['ANO_MES'] = df['DT_COMPTC'].dt.to_period('M')


# Função para calcular o retorno composto da média mensal do RETORNO_PEER
def retorno_composto_medio(grupo):
    # Calculando a média mensal do RETORNO_PEER
    media_mensal = grupo['RETORNO_PEER'].mean()

    # Calculando o retorno composto da média mensal
    retorno_comp = np.prod(1 + media_mensal) - 1

    return retorno_comp

# Função para calcular o retorno composto acumulado para um dado grupo.


def calc_retorno_composto_acumulado(grupo):
    # Garantir que os dados estão em ordem cronológica
    grupo = grupo.sort_values('ANO_MES')
    grupo['RETORNO_COMPOSTO_ACUMULADO'] = (
        1 + grupo['RETORNO_PEER']).cumprod() - 1
    return grupo


# Aqui é onde calculamos a soma da contribuição e a média do retorno do peer por gestor e mês
df_soma_contrib = df.groupby(['Gestor', 'ANO_MES']).agg({
    'CONTRIBUICAO': 'sum',
    'RETORNO_PEER': 'mean'
}).reset_index()


# Aplicando a função ao DataFrame agrupado por 'Gestor'
df_soma_contrib = df_soma_contrib.groupby('Gestor').apply(
    calc_retorno_composto_acumulado).reset_index(drop=True)

# Função para calcular o retorno composto acumulado de cada métrica


def calc_retorno_composto_acumulado(grupo):
    # Garantir que os dados estão em ordem cronológica
    grupo = grupo.sort_values('ANO_MES')
    grupo['RETORNO_COMPOSTO_ACUMULADO_PEER'] = (
        1 + grupo['RETORNO_PEER']).cumprod() - 1
    grupo['RETORNO_COMPOSTO_ACUMULADO_PESO'] = (
        1 + grupo['CONTRIBUICAO']).cumprod() - 1
    return grupo


# Aplicando a função ao DataFrame agrupado por 'Gestor'
df_soma_contrib = df_soma_contrib.groupby('Gestor').apply(
    calc_retorno_composto_acumulado).reset_index(drop=True)

# Calculando a diferença entre os retornos compostos acumulados
df_soma_contrib['DIFERENCA_RETORNO_COMPOSTO_ACUMULADO'] = df_soma_contrib['RETORNO_PEER'] - \
    df_soma_contrib['CONTRIBUICAO']

novas_linhas = []
for index, row in df_soma_contrib.iterrows():
    # Calcula a soma dos pesos para o mesmo gestor e mês
    soma_peso = df[(df['Gestor'] == row['Gestor']) & (
        df['ANO_MES'] == row['ANO_MES'])]['PESO'].sum()

    nova_linha = {
        'Gestor': row['Gestor'],
        'ANO_MES': row['ANO_MES'],
        'CONTRIBUICAO': row['DIFERENCA_RETORNO_COMPOSTO_ACUMULADO'],
        'FUNDO_AJUSTADO': 'Outros',  # adicionando o FUNDO_AJUSTADO
        'PESO': 1 - soma_peso  # calculando o novo peso
    }
    novas_linhas.append(nova_linha)

df_novas_linhas = pd.DataFrame(novas_linhas)

# Concatenando df_novas_linhas com df_final
df_final = pd.concat([df, df_novas_linhas], ignore_index=True)

# Merge df_final com df_soma_contrib para adicionar a coluna do retorno composto
df_final = pd.merge(df_final, df_soma_contrib[['Gestor', 'ANO_MES', 'RETORNO_COMPOSTO_ACUMULADO_PEER']],
                    on=['Gestor', 'ANO_MES'], how='left')

# Primeiro, encontre o último mês disponível em seu DataFrame
ultimo_mes = df_final['ANO_MES'].max()

# Filtra o DataFrame para incluir apenas entradas do último mês
df_final_ultimo_mes = df_final[df_final['ANO_MES'] == ultimo_mes]

# Agora a lista de gestores únicos será apenas daqueles presentes no último mês
gestores_unicos = df_final_ultimo_mes["Gestor"].unique()

# O resto do seu código se mantém praticamente o mesmo, mas você usará df_final_ultimo_mes no loop
for gestor in gestores_unicos:
    df_gestor = df_final_ultimo_mes[df_final_ultimo_mes["Gestor"] == gestor]

    # Agrupa por 'FUNDO_AJUSTADO' e soma as contribuições para o gestor atual
    df_gestor_agrupado = df_gestor.groupby("FUNDO_AJUSTADO")[
        "CONTRIBUICAO"].sum().reset_index()

    # Defina a variável max_retorno_composto como o retorno composto máximo para o gestor
    max_retorno_composto_gestor = df_gestor[df_gestor["ANO_MES"] == df_gestor["ANO_MES"].max(
    )]["RETORNO_PEER"].iloc[0]

    # Calcule a soma de CONTRIBUICAO para todos os fundos (exceto "Outros") para o gestor atual
    soma_peso_retorno_gestor = df_gestor_agrupado[df_gestor_agrupado["FUNDO_AJUSTADO"]
                                                  != "Outros"]['CONTRIBUICAO'].sum()

    # Atualize o valor de "Outros"
    df_gestor_agrupado.loc[df_gestor_agrupado["FUNDO_AJUSTADO"] == "Outros",
                           "CONTRIBUICAO"] = max_retorno_composto_gestor - soma_peso_retorno_gestor

    # Verifique se a linha "Outros" está presente. Se não estiver, adicione-a.
    if not (df_gestor_agrupado["FUNDO_AJUSTADO"] == "Outros").any():
        outros_df = pd.DataFrame(
            [{'FUNDO_AJUSTADO': 'Outros', 'CONTRIBUICAO': 0}])
        df_gestor_agrupado = pd.concat(
            [df_gestor_agrupado, outros_df], ignore_index=True)

    # Calcule o valor correto para "Outros" para o gestor atual
    soma_peso_retorno_gestor = df_gestor_agrupado[df_gestor_agrupado["FUNDO_AJUSTADO"]
                                                  != "Outros"]['CONTRIBUICAO'].sum()
    df_gestor_agrupado.loc[df_gestor_agrupado["FUNDO_AJUSTADO"] == "Outros",
                           "CONTRIBUICAO"] = max_retorno_composto_gestor - soma_peso_retorno_gestor

    # Reordene o DataFrame do gestor atual
    df_gestor_agrupado = df_gestor_agrupado.sort_values(
        'CONTRIBUICAO', ascending=False).reset_index(drop=True)

    # Crie o gráfico como anteriormente, mas para o gestor da iteração

    fig = go.Figure()

    labels = list(df_gestor_agrupado["FUNDO_AJUSTADO"])
    labels.append("Total")
    measure = ["relative" for _ in df_gestor_agrupado.index]
    measure.append("total")
    data = list(df_gestor_agrupado["CONTRIBUICAO"])
    data.append(df_gestor_agrupado["CONTRIBUICAO"].sum())

    fig.add_trace(go.Waterfall(
        orientation="v",
        measure=measure,
        x=labels,
        textposition="outside",
        increasing={"marker": {"color": colors[0]}},
        decreasing={"marker": {"color": colors[1]}},
        totals={"marker": {"color": colors[2]}},
        connector={"visible": False},
        text=[f"{i:.1%}" for i in data],
        y=data
    ))

    chart_layout = dict(
        width=900,
        height=650,
        font={"family": "Segoe UI", "size": 15},
        legend={"orientation": "h"},
        xaxis={"tickformat": ",", "showgrid": False, "zeroline": False},
        yaxis={"tickformat": ".1%", "showgrid": False, "zeroline": False},
        margin=dict(l=20, r=20, t=25, b=25),
        hovermode="x",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)')

    # Adicionando o nome do gestor no título
    # fig.update_layout(title=f"Attribution do Gestor: {gestor}")
    fig.update_layout(chart_layout)
    print("saving")
    fig.write_image(os.path.join(diretorio_base, f"{gestor} MTD.png"))
    print("saved")
    # fig.show()

# with pd.ExcelWriter('resultado_peers_go2_faltantes.xlsx', engine='xlsxwriter') as writer:
#     df_final.to_excel(writer, sheet_name='Sheet1', index=False)
#     workbook = writer.book
#     worksheet = writer.sheets['Sheet1']
#     date_format = workbook.add_format({'num_format': 'dd/mm/yyyy'})
#     worksheet.set_column('A:A', None, date_format)

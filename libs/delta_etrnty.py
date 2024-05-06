import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from workalendar.america import Brazil
from pandas.tseries.offsets import BMonthEnd
from dotenv import load_dotenv
import os
import numpy as np
import plotly.graph_objects as go
from libs.sinonimos import dicionario_sinonimos
import openpyxl
# from plotly.subplots import make_subplots

diretorio_base = os.path.join(".","figures")

tables = {"EON":["cvm_peers_fim","cvm_peers_fim_estimado","FIM"], "EVO":["cvm_peers_fia","cvm_peers_fia_estimado","FIA"]}

colors = ["#2C4257",
          "#48728A",
          "#708F92",
          "#A3ABA4",
          "#605869",  # cor principal  para texto de corpo
          "#948794",
          "#E7A75F",  # Apenas para detalhes em elementos gráficos
          "#A25B1E"  # Apenas em gráficos
          ]

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

# Função para calcular o retorno composto acumulado de cada métrica
def calc_retorno_composto_acumulado_peer(grupo):
    # Garantir que os dados estão em ordem cronológica
    grupo.sort_values('ANO_MES', inplace=True)

    # Inicializa as colunas para o cálculo do retorno composto acumulado
    grupo['RETORNO_COMPOSTO_ACUMULADO_PEER'] = 0.0
    grupo['RETORNO_COMPOSTO_ACUMULADO_PESO'] = 0.0

    # Variável para armazenar o retorno acumulado do ano atual, reinicia para cada novo ano
    retorno_acumulado_peer = 1.0
    retorno_acumulado_peso = 1.0

    # Ano do primeiro registro no grupo
    ano_anterior = grupo.iloc[0]['ANO_MES'].year

    for index, row in grupo.iterrows():
        # Se o ano da linha atual for diferente do 'ano_anterior', reinicia os cálculos
        if row['ANO_MES'].year != ano_anterior:
            retorno_acumulado_peer = 1.0
            retorno_acumulado_peso = 1.0
            ano_anterior = row['ANO_MES'].year

        retorno_acumulado_peer *= (1 + row['RETORNO_PEER'])
        retorno_acumulado_peso *= (1 + row['CONTRIBUICAO'])

        grupo.at[index, 'RETORNO_COMPOSTO_ACUMULADO_PEER'] = float(retorno_acumulado_peer - 1)
        grupo.at[index, 'RETORNO_COMPOSTO_ACUMULADO_PESO'] = float(retorno_acumulado_peso - 1)

    return grupo

# Função para filtrar por período MTD ou YTD
def filtrar_por_periodo(df, df_ultimo_mes, periodo):
    if periodo == "MTD":
        return df_ultimo_mes
    elif periodo == "YTD":
        return df[df['ANO_MES'].dt.year == df['ANO_MES'].max().year]
    else:
        raise ValueError("Período especificado é inválido. Escolha 'MTD' ou 'YTD'.")

def gera_df(fundo:str, periodo:str, save_files:bool=True)->pd.DataFrame:

#####  Configuração #########
    table=tables[fundo]
    periodo_configurado = periodo  # "MTD ou "YTD"

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

    query = "SELECT Gestor, DT_COMPTC, CNPJ_FUNDO, CNPJ_FUNDO_COTA, NM_FUNDO_COTA, PESO, RETORNO, RETORNO_PEER, CONTRIBUICAO FROM "+table[0]#+ " WHERE DT_COMPTC>'2023-12-31'"

    query2 = "SELECT Gestor, DT_COMPTC, CNPJ_FUNDO, CNPJ_FUNDO_COTA, NM_FUNDO_COTA, PESO, RETORNO, RETORNO_PEER, CONTRIBUICAO FROM "+table[1]#+ " WHERE DT_COMPTC>'2023-12-31'"

    query_combinada = query + ' UNION ALL ' + query2


    df = pd.read_sql(query_combinada, con=engine)

    # Cria a nova coluna com os valores da coluna original
    df['FUNDO_AJUSTADO'] = df['NM_FUNDO_COTA']

    # Faz as substituições com base no dicionário de sinônimos
    df['FUNDO_AJUSTADO'].replace(dicionario_sinonimos, inplace=True)

    # Ajustes de colunas para o merge e cálculos
    df['ANO_MES'] = df['DT_COMPTC'].dt.to_period('M')

    # Certifique-se de que o DataFrame 'df' está ordenado por 'ANO_MES'
    df = df.sort_values('ANO_MES')

    # Aqui é onde calculamos a soma da contribuição e a média do retorno do peer por gestor e mês
    df_soma_contrib = df.groupby(['Gestor', 'ANO_MES']).agg({
        'CONTRIBUICAO': 'sum',
        'RETORNO_PEER': 'mean'
    }).reset_index()


    # Aplicando a função ao DataFrame agrupado por 'Gestor'
    df_soma_contrib = df_soma_contrib.groupby('Gestor').apply(
        calc_retorno_composto_acumulado).reset_index(drop=True)

    # Aplicando a função ao DataFrame agrupado por 'Gestor'
    df_soma_contrib = df_soma_contrib.groupby('Gestor').apply(
        calc_retorno_composto_acumulado_peer).reset_index(drop=True)

    # Calculando a diferença entre os retornos compostos acumulados
    df_soma_contrib['DIFERENCA_RETORNO_COMPOSTO_ACUMULADO'] = df_soma_contrib['RETORNO_PEER'] - \
        df_soma_contrib['CONTRIBUICAO']

    novas_linhas = []
    for index, row in df_soma_contrib.iterrows():
        # Calcula a soma dos pesos para o mesmo gestor e mês
        soma_peso = df[(df['Gestor'] == row['Gestor']) & (
            df['ANO_MES'] == row['ANO_MES'])]['PESO'].sum()

        if soma_peso!=1:
            nova_linha = {
                'Gestor': row['Gestor'],
                'ANO_MES': row['ANO_MES'],
                'CONTRIBUICAO': row['DIFERENCA_RETORNO_COMPOSTO_ACUMULADO'],
                'FUNDO_AJUSTADO': 'Outros',  # adicionando o FUNDO_AJUSTADO
                'PESO': 1 - soma_peso  # calculando o novo peso
            }
            novas_linhas.append(nova_linha)
    df_novas_linhas = pd.DataFrame(novas_linhas)

    # Seu código para concatenar df_novas_linhas com df_final
    df_final = pd.concat([df, df_novas_linhas], ignore_index=True)

    # Seu código para merge df_final com df_soma_contrib para adicionar a coluna do retorno composto
    df_final = pd.merge(df_final, df_soma_contrib[['Gestor', 'ANO_MES', 'RETORNO_COMPOSTO_ACUMULADO_PEER']],
                        on=['Gestor', 'ANO_MES'], how='left')

    # Agrupando por 'ANO_MES' e calculando a média (ou outra agregação apropriada) de 'RETORNO_PEER'
    retorno_peer_etrnty = df_final[df_final['Gestor'] == 'Etrnty'].groupby('ANO_MES')['RETORNO_PEER'].mean()

    # Subtraindo o RETORNO_PEER da ETRNTY correspondente para cada linha em df_final
    df_final['CONTRIBUICAO_RELATIVA'] = df_final.apply(
        lambda row: ((row['CONTRIBUICAO']/row['PESO']) - retorno_peer_etrnty.get(row['ANO_MES'], 0)) * row['PESO'] , axis=1 #type: ignore
    )

    # Seu código para encontrar o último mês disponível em seu DataFrame
    ultimo_mes = df_final['ANO_MES'].max()

    # Seu código para filtrar o DataFrame para incluir apenas entradas do último mês
    df_final_ultimo_mes = df_final[df_final['ANO_MES'] == ultimo_mes]

    # df_final.to_excel("check.xlsx", index=False)

    # Aqui você usaria a função para pegar o DataFrame apropriado
    df_periodo_especifico = filtrar_por_periodo(df_final, df_final_ultimo_mes, periodo_configurado)

    # Seu código para criar a lista de gestores únicos do período especificado
    gestores_unicos = df_periodo_especifico["Gestor"].unique()

    # Seu loop para processar informações para cada gestor
    for gestor in gestores_unicos:
        # Filtrando os dados pelo gestor da iteração
        df_gestor = df_periodo_especifico[df_periodo_especifico["Gestor"] == gestor]

        # Agrupa por 'FUNDO_AJUSTADO' e soma as contribuições para o gestor atual
        df_gestor_agrupado = df_gestor.groupby("FUNDO_AJUSTADO")["CONTRIBUICAO_RELATIVA"].sum().reset_index()

        if periodo_configurado == "MTD":
            # Se MTD, utiliza a coluna 'RETORNO_PEER'
            max_retorno_composto_gestor = df_gestor[df_gestor["ANO_MES"] == df_gestor["ANO_MES"].max()]["RETORNO_PEER"].iloc[0]
        elif periodo_configurado == "YTD":
            # Se YTD, utiliza a coluna 'RETORNO_COMPOSTO_ACUMULADO_PEER'
            max_retorno_composto_gestor = df_gestor[df_gestor["ANO_MES"] == df_gestor["ANO_MES"].max()]["RETORNO_COMPOSTO_ACUMULADO_PEER"].iloc[0]

            # Obter o retorno composto acumulado máximo do peer da ETRNTY na data mais recente
            data_max_etrnty = df_soma_contrib[df_soma_contrib["Gestor"] == "Etrnty"]["ANO_MES"].max()
            max_retorno_composto_etrnty = df_soma_contrib[(df_soma_contrib["Gestor"] == "Etrnty") & (df_soma_contrib["ANO_MES"] == data_max_etrnty)]["RETORNO_COMPOSTO_ACUMULADO_PEER"].iloc[0]

            # Calcular a diferença
            diferenca_retorno = max_retorno_composto_gestor - max_retorno_composto_etrnty

            # Subtrair a soma da contribuição relativa do gestor atual
            soma_contribuicao_relativa_gestor = df_gestor["CONTRIBUICAO_RELATIVA"].sum()
            diferenca_ajustada = diferenca_retorno - soma_contribuicao_relativa_gestor

            # Atualizar o valor de 'Outros'
            valor_outros_atual = df_gestor_agrupado[df_gestor_agrupado["FUNDO_AJUSTADO"] == "Outros"]["CONTRIBUICAO_RELATIVA"].iloc[0]
            df_gestor_agrupado.loc[df_gestor_agrupado["FUNDO_AJUSTADO"] == "Outros", "CONTRIBUICAO_RELATIVA"] = valor_outros_atual + diferenca_ajustada
        else:
            raise ValueError("Período especificado é inválido. Escolha 'MTD' ou 'YTD'.")

        # Reordene o DataFrame do gestor atual
        df_gestor_agrupado = df_gestor_agrupado.sort_values('CONTRIBUICAO_RELATIVA', ascending=False).reset_index(drop=True)
        
        # Identificar os fundos da ETRNTY
        fundos_etrnty = set(df_final_ultimo_mes[df_final_ultimo_mes['Gestor'] == 'Etrnty']['FUNDO_AJUSTADO'])

        if save_files is True:
            # Crie o gráfico como anteriormente, mas para o gestor da iteração

            fig = go.Figure()

            # Modificação aqui: verificação e formatação em negrito
            labels = [f"<b>{fundo}</b>" if fundo in fundos_etrnty else fundo for fundo in df_gestor_agrupado["FUNDO_AJUSTADO"]]
            labels.append("Total")

            measure = ["relative" for _ in df_gestor_agrupado.index]
            measure.append("total")

            data = list(df_gestor_agrupado["CONTRIBUICAO_RELATIVA"])
            data.append(df_gestor_agrupado["CONTRIBUICAO_RELATIVA"].sum())

            fig.add_trace(go.Waterfall(
                orientation="v",
                measure=measure,
                x=labels,
                textposition="auto",
                increasing={"marker": {"color": colors[0]}},
                decreasing={"marker": {"color": colors[3]}},
                totals={"marker": {"color": colors[6]}},
                connector={"visible": False},
                text=[f"<b>{i:.1%}</b>" for i in data],
                y=data

            ))

            chart_layout = dict(
                    height=600,
                    width=650,
                    font={"family": "Segoe UI", "size": 14},
                    legend={"orientation": "h"},
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
            
            #if gestor == "Etrnty" and periodo_configurado == "MTD":  # Corrigindo a sintaxe
            #    chart_layout['yaxis']['range'] = [-0.001, 0.014]  # Definindo o intervalo para -0.01% a 1.4%  #type: ignore
            #elif gestor == "Etrnty" and periodo_configurado == "YTD":
            #    chart_layout['yaxis']['range'] = [-0.003, 0.025]  # Definindo o intervalo para -0.01% a 1.4%  #type: ignore

            # Adicionando o nome do gestor no título
            # fig.update_layout(title=f"Attribution do Gestor: {gestor}")
            fig.update_layout(chart_layout)
            fig.write_image(os.path.join(diretorio_base, f"{table[2]}_{gestor}_{periodo_configurado}.png"))
            # fig.show()
    return df_final_ultimo_mes

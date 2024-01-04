"""
Le arquvios com posições CVM dos fundos dos concorrentes e compara sua 
composição com a do nosso fundo (baixado do meuportfolio)
"""
# pylint: disable=invalid-name
import os
import pandas as pd
import plotly.graph_objects as go
from libs.delta_etrnty import gera_df
from libs.db_functions import fetch_all_fundos
from et_lib.ET_Meu_portfolio import Meu_portfolio_connection


sinonimos= fetch_all_fundos()

def read_my_portfolio(fund_name:str, fund_pl:pd.DataFrame, ETR_CNPJ:str)->pd.DataFrame:
    """
    Lê carteira do fundo do meu portfólio e compatibiliza para concatenar com o arquivo lido da CVM
    """
    meu_portfolio_connection=Meu_portfolio_connection(False)
    dt=meu_portfolio_connection.get_portfolio_last_date(fund_name)
    my_fund=meu_portfolio_connection.get_portfolio_positions_as_df(fund_name,dt)
    my_fund=my_fund.loc[my_fund["Tipo ID"]=="CNPJ"]
    my_fund["TP_FUNDO"]=["FI" for _ in my_fund.index]
    my_fund["CNPJ_FUNDO"]=[ETR_CNPJ for _ in my_fund.index]
    my_fund["DENOM_SOCIAL"]=["my_fund MASTER" for _ in my_fund.index]
    my_fund["TP_ATIVO"]=["COTA DE FUNDO" for _ in my_fund.index]
    my_fund["VL_MERC_POS_FINAL"]=my_fund["Valor"]
    my_fund["CNPJ_FUNDO_COTA"]=my_fund["ID"]
    #my_fund["NM_FUNDO_COTA"]=["3 ILHAS FIC FIA" for _ in my_fund.index]

    #Preenche NM_FUNDO_COTA
    new_names=fund_pl.loc[fund_pl["CNPJ_FUNDO"].isin(my_fund["CNPJ_FUNDO_COTA"])]
    for it_cnpj in new_names["CNPJ_FUNDO"]:
        if my_fund.loc[my_fund["CNPJ_FUNDO_COTA"]==it_cnpj].empty is False:
            my_fund.loc[my_fund["CNPJ_FUNDO_COTA"]==it_cnpj,"NM_FUNDO_COTA"]=new_names.loc[new_names["CNPJ_FUNDO"]==it_cnpj,"DENOM_SOCIAL"]
    return my_fund

def make_heatmap(fund:str, df:pd.DataFrame):
    result=df.copy()
    result.reset_index(drop=True, inplace=True)
    pivot_table = result.pivot_table(index='FUNDO_AJUSTADO', columns='Gestor', values='PESO', aggfunc='sum').fillna(0)
    pivot_table=pivot_table[pivot_table.index!="Outros"]
    x=pivot_table[pivot_table>0].count(axis=1).sort_values(ascending=False).index
    pivot_table=pivot_table.loc[x]
    idx=pivot_table.index
    if True:
        idx=[txt.replace("INVESTIMENTO EM AÇÕES","") for txt in idx]
        idx=[txt.replace("INVESTIMENTO NO EXTERIOR","") for txt in idx]
        idx=[txt.replace(" FUNDO DE INVESTIMENTO EM AÇÕES","") for txt in idx]
        idx=[txt.replace(" FUNDO DE INVESTIMENTO MULTIMERCADO","") for txt in idx]
        idx=[txt.replace(" FUNDOS DE INVESTIMENTO MULTIMERCADO","") for txt in idx]
        idx=[txt.replace(" FUNDO DE INVESTIMENTO EM COTAS","") for txt in idx]
        idx=[txt.replace(" CRÉDITO PRIVADO","") for txt in idx]
        idx=[txt.replace(" FUNDO DE INVESTIMENTO","") for txt in idx]
        idx=[txt.replace(" DE AÇÕES","") for txt in idx]
        idx=[txt.replace(" DE ACOES","") for txt in idx]
        idx=[txt.replace(" FUNDO DE","") for txt in idx]
    pivot_table.index=pd.Index(idx )
    fundos_na_carteira = pivot_table[pivot_table>0].count()
    investidores_no_fundo = pivot_table[pivot_table>0].count(axis=1)

    pivot_table.index=pd.Index([str(i).strip()+" ("+str(investidores_no_fundo[i])+")" for i in pivot_table.index])
    pivot_table.columns=[str(i).strip()+" ("+str(fundos_na_carteira[i])+")" for i in pivot_table.columns]
    custom_colorscale = [
        [0, "#ffffff"],   # Colorscale etrnty
        [1, "#2C4257"]
    ]

    heatmap= data=go.Heatmap(
                        z=pivot_table.transpose(),
                        y=pivot_table.columns,
                        x=pivot_table.index,
                        colorscale=custom_colorscale,
                        xgap=1,
                        ygap=1)

    fig = go.Figure(data=[heatmap])
    fig.update_layout(yaxis_nticks=len(pivot_table.columns),
                      yaxis={"dtick":1},
                      xaxis={"tickangle":90},
                      width=2000, height=820,
                      plot_bgcolor='rgba(0,0,0,0)',
                      paper_bgcolor='rgba(0,0,0,0)')

    fig.write_image(os.path.join(".","figures","heatmap_"+fund+".png"))


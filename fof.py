from __future__ import annotations
from typing import Literal, List
import pandas as pd
import os
from datetime import datetime
from summary import last_day_of_previous_month
import plotly.graph_objects as go
from et_lib.ET_Data_Reader import BasketHistoricalData
from gb import Carteira
from settings import colors, chart_layout, vertical_layout, fund_data, startDate, endDate

   
def plot_returns(df:pd.DataFrame):
    rets=(df.shift(-1)/df-1).dropna()
    fig=go.Figure(data=[
        go.Bar(name="Relized-Expected", x=df.index, y=(rets["expected"]-rets["realized"]),marker_color=colors[0]),
    ])
    #fig.update_layout(chart_layout)
    fig.update_layout(title="Retorno experado - realizado (dia a dia)",
                      yaxis_tickformat='.2%')
    return fig

def plot_dict_as_bar(d:dict, fund_name:str):
    x=[i.replace("FIC","").replace("FIA","").replace("FIM","").rstrip() for i in d.keys()]
    fig=go.Figure(data=[
        go.Bar(name="Performance", x=x, y=list(d.values()),
               marker_color=[colors[7] if i =="IBX" or i == "IFMM" else (colors[6] if i==fund_name else colors[0]) for i in d.keys()],
               text=["{:.1%}".format(i) for i in list(d.values())])
    ])
    fig.update_layout(chart_layout, yaxis_tickformat='.2%')
    return fig

def plot_contributions(contr:dict):
    x=[i.replace("FIC","").replace("FIA","").replace("FIM","").rstrip() for i in contr.keys()]
    fig=go.Figure(go.Waterfall(name="Contribuição de retorno", orientation="v",
                               measure=["relative" for _ in contr][:-1]+["total"],
                               x = x,
                              # textposition="inside",
                               text=["{:.1%}".format(contr[i]) for i in contr],
                               y = list(contr.values()),
                               connector=None,
                               increasing={"marker":{"color":colors[0]}},
                               decreasing={"marker":{"color":colors[3]}},
                               totals={"marker":{"color":colors[6]}}
                               ))
    fig.update_layout(chart_layout, yaxis_tickformat='.2%')
    return fig

def move_to_bottom(df:pd.Series, idx:str)->pd.Series:
    x=df.loc[idx]
    df=df.drop(idx)
    df=pd.concat([df,pd.Series({idx:x})])
    return df

def cnpj_or_name(cnpj_dict:dict, cnpj:str)->str:
    if cnpj in cnpj_dict.keys():
        key=cnpj_dict[cnpj]
    else:
        key=cnpj
    return key

def plot_weight_changes(diff:pd.DataFrame, fund:str):
    vars=diff.copy()
    vars.index=pd.Index([i.replace("FIM","").replace("FIA","").replace("FIC","") for i in vars.index])
    fig = go.Figure(go.Bar(x=vars["weight"],y=vars.index,orientation="h",width=0.5,marker_color=colors[0],text=["{:.1%}".format(i) for i in list(vars["weight"])]))
    fig.update_layout(vertical_layout)
    fig.update_xaxes(range=[-0.25, 0.25])
    fig.write_image(os.path.join(".","figures",fund+"_weight_cng.png"),scale=2)

def limpa_dataframe(df:pd.DataFrame)->pd.DataFrame:
    columns=['ValorBruto', 'cod_etrnty', 'CNPJ']
    dynamo=["Dynamo Cougar Advisory FIC FIA","Dynamo Cougar Advisory I FIC FIA","Dynamo Cougar Advisory II FIC FIA"]
    df.loc[df["cod_etrnty"].isin(dynamo),"cod_etrnty"]="Dynamo Cougar"
    df=df.loc[df["DescricaoTipoPosicao"]!="Patrimonio"]
    df=df.loc[df["Blotter"]==False]
    df.loc[df["DescricaoTipoPosicao"].isin(["Conta Corrente - CPR","Taxa de administração","Taxa de custódia","Conta Corrente - Saldo"]),"tags"]="Caixa"
    df.loc[df["DescricaoTipoPosicao"].isin(["Conta Corrente - CPR","Taxa de administração","Taxa de custódia","Conta Corrente - Saldo"]),"cod_etrnty"]="Caixa"
    df.loc[df["tags"].str.contains("Caixa"),"cod_etrnty"]="Caixa"
    df=df[columns]
    df=df.groupby(by="cod_etrnty").sum()
    valor_total=df["ValorBruto"].sum()
    df["weight"]=df["ValorBruto"]/valor_total
    if "BOVA11" in df.index:
        df.loc["BOVA11","CNPJ"]="10406511000161"
    if "Dynamo Cougar" in df.index:
        df.loc["Dynamo Cougar","CNPJ"]="44769980000167"
    return df

def performance_attrib_fof(fof_name=Literal["EON"]|Literal["EVO"])->List[go.Figure]:
    df=[]
    fund=fund_data[str(fof_name)]
    print(fof_name+" baixando carteiras da britech.")
    df0=Carteira.get_posicao_carteira(ids_carteira=fund["cod_britech"],date_pos=startDate,cod_etr=fof_name)
    df1=Carteira.get_posicao_carteira(ids_carteira=fund["cod_britech"],date_pos=endDate,cod_etr=fof_name)

    cnpj_dict={df0.loc[i,"CNPJ"]:str(df0.loc[i,"cod_etrnty"]) for i in df0.index}|{df1.loc[i,"CNPJ"]:str(df1.loc[i,"cod_etrnty"]) for i in df1.index}
    cnpj_dict[fund["fund_cnpj"]]=str(fof_name)
    cnpj_dict["10406511000161"]="BOVA11"
    cnpj_dict["44769980000167"]="Dynamo Cougar"
    cnpj_dict["CDI"]="Caixa"
    cnpj_dict["IFMM BTG Pactual"]="IFMM"
    
    df.append(limpa_dataframe(df0))
    df.append(limpa_dataframe(df1))
    
    diff=pd.DataFrame(df[1]["weight"].subtract(df[0]["weight"],fill_value=0)).sort_values(by="weight")
    plot_weight_changes(diff,str(fof_name))
    df[0].loc["Caixa","CNPJ"]="CDI"
    df[0].loc[fund["additional_member1"][0]]=[0,fund["additional_member1"][2],0]
    df[0].loc[fund["additional_member2"][0]]=[0,fund["additional_member2"][2],0]
    basket=[{"Ticker":df[0].loc[i,"CNPJ"],"Source":"Quantum"} for i in df[0].index]
    d=BasketHistoricalData("Port",startDate,endDate,basket)
    print("Downloading data from quantum.")
    precos=d.getData()
    precos=precos.droplevel(level=1,axis=1)
    cum_rets=precos/precos.iloc[0]

    ws=df[0].loc[df[0]["CNPJ"].isin(cum_rets.columns),"weight"]
    cum_rets.columns=[cnpj_or_name(cnpj_dict,i) for i in cum_rets.columns]
    total_weight=ws.sum()
    ws=ws/total_weight
    expected=cum_rets.dot(ws)
    realized=cum_rets[fof_name]
    sdf=pd.concat([expected,realized],axis=1)
    sdf.columns=["expected","realized"]

    rets=(cum_rets.iloc[-1]-1)
    contr=rets*ws
    fig=plot_returns(sdf)
    fig.write_image(os.path.join(".","figures",str(fof_name)+"_tracking.png"),scale=2)

    rets=rets.sort_values()
    rets.index=[cnpj_or_name(cnpj_dict, i).rstrip() for i in rets.index]
    rets=move_to_bottom(rets,fund["additional_member2"][0])
    rets=move_to_bottom(rets,str(fof_name))

    contr=contr.sort_values()
    contrib={}
    performance={}
    for i in contr.index:
        if cnpj_or_name(cnpj_dict,i)!=fund["additional_member2"][0] and cnpj_or_name(cnpj_dict,i)!=fund["fund_name"]:
            if contr.loc[i]!=0:
                contrib[cnpj_or_name(cnpj_dict,i)]=contr.loc[i]
    
    for i in rets.index:
        performance[cnpj_or_name(cnpj_dict,i)]=rets.loc[cnpj_or_name(cnpj_dict,i)]

    contrib["Erro"]=(sdf["realized"]-sdf["expected"]).iloc[-1]
    contrib["Total"]=sdf["realized"].iloc[-1]-1
    fig1=plot_contributions(contrib)
    #fig.show()
    fig1.write_image(os.path.join(".","figures",str(fof_name)+"_contribution.png"),scale=2)

    fig2=plot_dict_as_bar(performance,str(fof_name))
    #fig.show()
    fig2.write_image(os.path.join(".","figures",str(fof_name)+"_price_cngs.png"),scale=2)
    return [fig1,fig2]


if __name__=="__main__":
    columns=['ValorBruto', 'cod_etrnty', 'CNPJ'] #'Ativo', 'IdAtivo','DescricaoTipoPosicao', 'tags'

        

"""
Prepara os gráficos de barra comparando a performance dos nossos FOFs contra os pares da indústria
"""
# pylint: disable=invalid-name
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long
# pylint: disable=consider-using-dict-items
# pylint: disable=consider-iterating-dictionary
# pylint: disable=wrong-import-position
# pylint: disable=import-error

import os
from typing import List
import datetime
import holidays
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from et_lib.ET_Data_Reader import  BasketHistoricalData
from cvm_fim.atualizar_db.atualizar_cvm_peers_FIM import FIMS
from cvm_fia.atualizar_db.atualizar_cvm_peers_FIA import FIAS

#import statsmodels.api as sm

holiday_gen=holidays.country_holidays("BR", subdiv="SP")

chart_layout = dict(
    width=900,
    height=650,
    font={"family":"Segoe UI","size":15},
    legend={"orientation":"h"},
    xaxis= {"tickformat":",","showgrid":False, "zeroline":False},
    yaxis= {"tickformat":".2s","showgrid":False, "zeroline":False},
    margin=dict(l=20, r=20, t=25, b=25),
    hovermode = "x",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)


colors=["#2C4257",
        "#48728A",
        "#708F92",
        "#A3ABA4",
        "#605869",   #cor principal  para texto de corpo
        "#948794",
        "#E7A75F",   #Apenas para detalhes em elementos gráficos
        "#A25B1E"    #Apenas em gráficos
        ]

def n_years_ago(data:datetime.datetime, n:int):
    if data.month==2 and data.day==29 and n%4!=0:
        return datetime.datetime(data.year-n,2,28)
    if data.month==2 and data.day==28 and n%4==0:
        return datetime.datetime(data.year-n,2,29)
    return datetime.datetime(year=data.year-n,month=data.month,day=data.day)

def last_day_of_previous_month(today:datetime.datetime, index: List[datetime.datetime])->datetime.datetime:
    """
    Retorna o ultimo dia do mes anterior a data fornecida
    """
    #iterate backwards until the month is differente from today's month
    for i in range(len(index)-1,0,-1):
        if index[i].month != today.month:
            return index[i]
    raise ValueError("nao foi possivel encontrar o ultimo dia do mes anterior a "+str(today))

def last_day_of_previous_year(today:datetime.datetime, index: List[datetime.datetime])->datetime.datetime:
    """
    Retorna o ultimo dia do anterior anterior a data fornecida
    """
    #iterate backwards until the month is differente from today's month
    for i in range(len(index)-1,0,-1):
        if index[i].year != today.year:
            return index[i]
    raise ValueError("nao foi possivel encontrar o ultimo dia do ano anterior a "+str(today))

def day_before_date(date:datetime.datetime, index: List[datetime.datetime])->datetime.datetime|None:
    """
    returns date if date is in the index, otherwise returns the last date before date
    """
    if date in index:
        return date
    else:
        if date<index[0]:  #type: ignore
            return None
        else:
            #iterate index backwards until the value of index is smaller than date
            for i in range(len(index)-1,0,-1):
                if index[i]<date:
                    return index[i]

def get_price_change(ativos:list, end_date: datetime.datetime)->pd.DataFrame:
    """
    Retorna a variação de preço dos ativos em relação em um dicionário:
    "MTD", "YTD", "12 meses", "24 meses", "60 meses"
    """
    cnpj_dict={i["Ticker"]:i["Nome"] for i in ativos}
    start_date = datetime.datetime(end_date.year-6,end_date.month,1)
    try:
        q = BasketHistoricalData("fundo", start_date, end_date, ativos)
    except ValueError as exc:
        raise ValueError("nao foi possivel carregar dados para os ativos: "+' '.join(ativos)) from exc

    data_frame=q.getData(dropna=False)
    if data_frame.dropna().index[-1]!=end_date:
        raise ValueError("Não foi possível baixar todos os preços até o dia: "+end_date.strftime("%Y-%m-%d"))
    data_frame=data_frame.droplevel(axis=1,level=1)


    #data_frame.index=[i.to_pydatetime() for i in data_frame.index]
    data_frame.index = pd.to_datetime(data_frame.index)
    data_frame=data_frame.dropna(how="all")
    end_date=data_frame.index[-1] #type: ignore

    dates={
        "MTD":{"data":last_day_of_previous_month(end_date, list(data_frame.index))},
        "YTD":{"data":last_day_of_previous_year(end_date, list(data_frame.index))},
        "12 meses":{"data":day_before_date(n_years_ago(end_date,1), list(data_frame.index))},
        "24 meses":{"data":day_before_date(n_years_ago(end_date,2), list(data_frame.index))},
        "60 meses":{"data":day_before_date(n_years_ago(end_date,5), list(data_frame.index))}
    }
    rets={cnpj_dict[i]:{} for i in data_frame.columns}
    for ativo in data_frame.columns:
        preco_final=data_frame.loc[end_date,ativo]
        for d in dates.keys():
            if d not in rets[cnpj_dict[ativo]].keys():
                rets[cnpj_dict[ativo]][d]={"data":dates[d]["data"],"retorno":None}
            if rets[cnpj_dict[ativo]][d]["data"] is not None:
                preco_inicial=data_frame.loc[rets[cnpj_dict[ativo]][d]["data"],ativo]
                #check if preco_inicial is nan
                if pd.isna(preco_inicial):
                    rets[cnpj_dict[ativo]][d]["retorno"]=None
                else:
                    #format as % with 2 decimals
                    rets[cnpj_dict[ativo]][d]["retorno"]=round(((preco_final/preco_inicial)-1)*100,1)

    #convert rets to a dataframe, discard "data"
    rets=pd.DataFrame.from_dict({outer_key: {inner_key: rets[outer_key][inner_key]["retorno"] for inner_key in rets[outer_key].keys()} for outer_key in rets.keys()},orient="index")
    return rets


def get_FOF_price_change(basket:list, as_of:datetime.datetime):
    """
        args:
            FOF       : str "EON" | "EVO"
            as_of     : "yyyy-mm-dd"
    """

    r=get_price_change(basket,as_of)
    return r

def ticker_to_name(ticker:str, basket:list)->str:
    """
    returns the name of the fund with ticker
    """
    for i in basket:
        if i["Ticker"]==ticker:
            return i["Nome"]
    raise ValueError("ticker not found: ",ticker)

def get_changes_chart(title: str, start_date:datetime.datetime, end_date:datetime.datetime, df:pd.DataFrame, basket:list)->go.Figure:
    """
    returns a bar chart with the performance of the funds in df between start_date and end_date
    """
    rets=(df.loc[end_date]/df.loc[start_date]-1).droplevel(1)
    new_index=pd.Index([ticker_to_name(i,basket) for i in rets.index])
    rets.index=new_index
    #sort dataframe by values
    rets=rets.sort_values(ascending=True)  #type:ignore

    chart_colors=[colors[6] if i =="Etrnty" else (colors[7] if (i=="IBX" or i=="IFMM" or i=="CDI") else (colors[0] if rets.loc[i]>rets["Etrnty"] else colors[1])) for i in rets.index]

    #place value on top of bars, format as % tilted 90 degrees
    texts=[str(round(rets.loc[i]*100,1))+"%" for i in rets.index]
    fig=go.Figure(data=[
        go.Bar(name="Performance", x=list(rets.index), y=list(rets.values), marker_color=chart_colors, text=texts, textposition='outside')
    ])

    fig.update_layout(barmode='group', bargap=0.4)
    fig.update_layout(chart_layout)
    fig.update_layout(title=title,yaxis_tickformat='.1%')
    fig.update_layout(margin=dict(t=100))
    fig.update_xaxes( tickangle=0)
    fig.update_yaxes(showticklabels=False)

    return fig

def get_error_figure(text: str)->go.Figure:
    """
    returns a bar chart with the performance of the funds in df between start_date and end_date
    """
    fig=go.Figure()
    fig.add_annotation(x=0.1,y=0.5,text=text,
                       xref='paper',
                       yref='paper',
                       yanchor='middle')

    fig.update_layout(chart_layout)
    fig.update_layout(margin=dict(t=100))
    return fig


def get_beta_chart(title: str, start_date:datetime.datetime, end_date:datetime.datetime, df:pd.DataFrame, basket:list,beta:dict)->go.Figure:
    """
    returns a bar chart with the performance of the funds in df between start_date and end_date
    """
    rets=df.loc[end_date]/df.loc[start_date]-1
    rets=rets.sort_values(ascending=True)
    betas=[beta[i[0]] for i in rets.index]

    new_index=pd.Index([ticker_to_name(i[0],basket) for i in rets.index])
    rets.index=new_index
    #sort dataframe by values

    chart_colors=[colors[6] if i =="Etrnty" else (colors[7] if (i=="IBX" or i=="IFMM") else (colors[0] if rets.loc[i]>rets["Etrnty"] else colors[1])) for i in rets.index]

    #place value on top of bars, format as % tilted 90 degrees
    fig=go.Figure(data=[
        #scatter chart with betas in the X axis and rets in the Y axis
        #dont't connect points
        #write the fund name on the point

        go.Scatter(name="Performance", x=betas, y=list(rets.values), marker_color=chart_colors, text=rets.index,mode="markers+text",textposition='top center',textfont=dict(size=10))
    ])

    fig.update_layout(chart_layout)
    #format X axis with 1 decimal
    #add x axis title

    fig.update_layout(title=title,yaxis_tickformat='.1%',xaxis_tickformat='.2',xaxis_title="beta")
    fig.update_layout(margin=dict(t=100))
    return fig

def make_summary_figs(end_date:datetime.datetime, gestores:list)->List[go.Figure]:
    peers_evo=[{"Nome":i[1],"Ticker":i[0].replace(".","").replace("/","").replace("-",""),"Source":"Quantum"} for i in FIAS if i[1] in gestores]+[{"Nome":"IBX","Ticker":"IBX","Source":"Quantum"}]
    peers_eon=[{"Nome":i[1],"Ticker":i[0].replace(".","").replace("/","").replace("-",""),"Source":"Quantum"} for i in FIMS if i[1] in gestores]+[{"Nome":"IFMM","Ticker":"IFMM BTG PACTUAL","Source":"Quantum"},{"Nome":"CDI","Ticker":"CDI","Source":"Quantum"} ]

    print("Make summary figs running...")
    YTD_date=datetime.datetime(2023,12,29)

    it={"eon":peers_eon,"evo":peers_evo}
    tit={"eon":"Multimercado","evo":"Ações"}
    figs=[]

    for k in it.keys():
        d=BasketHistoricalData("Port",YTD_date,end_date,it[k])
        df=d.getData(dropna=False)
        
        rets=df/df.shift(1)-1
        rets=rets.droplevel(1,axis=1).dropna(how="any")
        if rets.index[-1]!=end_date:
            x=[i[0] for i in df.columns if np.isnan(df.iloc[-1][i])]
            s="Não foi possível baixar todos os preços até o dia: "+end_date.strftime("%Y-%m-%d")+". Ultima data: "+rets.index[-1].strftime("%Y-%m-%d")+"\nAtivos sem preço no final:"
            for i in x:
                s+="\n"+i
            fig=get_error_figure(s)
            figs.append(fig)
            fig.write_image(os.path.join(".","figures",k+"_YTD.png"))
            figs.append(fig)
            fig.write_image(os.path.join(".","figures",k+"_MTD.png"))
            print(s)
        else:
            fig=get_changes_chart(tit[k]+" - Peformance YTD",YTD_date,end_date,df,it[k])
            if os.path.exists(os.path.join(".","figures")):
                figs.append(fig)
                fig.write_image(os.path.join(".","figures",k+"_YTD.png"))
                print("Saved image:"+os.path.join(".","figures",k+"_YTD.png"))
            
            start_date_mtd=last_day_of_previous_month(end_date, list(df.index))
            fig=get_changes_chart(tit[k]+" - Peformance MTD",start_date_mtd,end_date,df,it[k])
            if os.path.exists(os.path.join(".","figures")):
                figs.append(fig)
                fig.write_image(os.path.join(".","figures",k+"_MTD.png"))
                print("Saved image:"+os.path.join(".","figures",k+"_MTD.png"))
    return figs

if __name__=="__main__":
    gestores = ["Brain", "Consenso", "Etrnty", "G5", "JBFO", "Mandatto", "Portofino", "Pragma", "Taler", "Vitra", "Warren", "Wright", "XPA"]
    end_date=datetime.datetime.strptime("2023-11-30","%Y-%m-%d")
    make_summary_figs(end_date,gestores)
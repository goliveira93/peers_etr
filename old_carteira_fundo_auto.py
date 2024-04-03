"""
Le arquvios com posições CVM dos fundos dos concorrentes e compara sua 
composição com a do nosso fundo (baixado do meuportfolio)
"""
# pylint: disable=invalid-name
import os
import pandas as pd
import plotly.graph_objects as go
from db_functions import fetch_all_fundos
from et_lib.ET_Meu_portfolio import Meu_portfolio_connection

sinonimos= fetch_all_fundos()

FIAS=[["26.470.647/0001-70","Mandatto"],
      ["13.958.690/0001-38","Taler"],
      ["07.875.686/0001-03","Consenso"],
      ["37.703.536/0001-83","Warren"],
      ["34.633.416/0001-69","Vitra"],
      ["06.128.183/0001-01","JBFO"],
      ["24.371.991/0001-87","Wright"],
      ["11.389.643/0001-95","Pragma"],
      ["35.610.530/0001-36","Brain"],
      ["18.060.935/0001-29","G5"],
      ["25.098.042/0001-38","XPA"],
      ["37.227.781/0001-61","Portofino"],
      ["47.700.200/0001-10","Etrnty"]]

FIMS=[["26.470.596/0001-87","Mandatto"],
      ["07.382.415/0001-16","Taler"],
      ["20.969.330/0001-05","Consenso"],
      ["46.615.722/0001-51","Warren"],
      ["34.633.424/0001-05","Vitra"],
      ["05.778.214/0001-07","JBFO"],
      ["22.884.922/0001-41","Wright"],
      ["11.389.633/0001-50","Pragma"],
      ["34.617.263/0001-66","Brain"],
      ["04.869.180/0001-01","G5"],
      ["28.777.487/0001-32","XPA"],
      ["36.727.650/0001-80","Portofino"],
      #["32.254.387/0001-07","São João"],
      ["47.716.356/0001-90","Etrnty"]]

config = {"EON":{"ETR_CNPJ":"47.716.356/0001-90", "meu_portfolio":"ETRNTY EON MM MASTER FIC FIM", "PEERS":FIMS, "filename":"EON" },
          "EVO":{"ETR_CNPJ":"47.700.200/0001-10", "meu_portfolio":"ETRNTY EVO FIC FIM", "PEERS":FIAS, "filename":"EVO" }
          }

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

if __name__=="__main__":
    file="cda_fi_BLC_2_202308.csv"
    pl_file="cda_fi_PL_202308.csv"
    conf=config["EVO"] #config["EON"]
    
    pl=pd.read_csv(os.path.join(".","data",pl_file),delimiter=";",encoding="ISO-8859-1")
    
    ETR=read_my_portfolio(conf["meu_portfolio"],pl, conf["ETR_CNPJ"])
    pl=pl[["CNPJ_FUNDO","VL_PATRIM_LIQ"]]
    pl=pl.set_index("CNPJ_FUNDO")
    pl.loc[conf["ETR_CNPJ"]]=ETR["VL_MERC_POS_FINAL"].sum()

    df=pd.read_csv(os.path.join(".","data",file),delimiter=";",encoding="ISO-8859-1")
    df=df[["TP_FUNDO","CNPJ_FUNDO","DENOM_SOCIAL","TP_ATIVO","VL_MERC_POS_FINAL","CNPJ_FUNDO_COTA","NM_FUNDO_COTA"]]
    df=df.drop(df[df["CNPJ_FUNDO"]==conf["ETR_CNPJ"]].index)
    ETR=ETR[df.columns]
    df=pd.concat([df,ETR],axis=0)
    result=pd.DataFrame()
    for f in conf["PEERS"]:
        fundo=f[0]
        sub_df=df.loc[df["CNPJ_FUNDO"]==fundo].copy()
        if sub_df.empty is True:
            continue

        sub_df["PESO"]=sub_df["VL_MERC_POS_FINAL"]/float(pl.loc[fundo,"VL_PATRIM_LIQ"])  #type: ignore
        pd.set_option('display.max_colwidth', 100)

        while True:
            sub_df_before=sub_df.copy()
            sub_df["IS_FEEDER"]=sub_df["CNPJ_FUNDO_COTA"].isin(df["CNPJ_FUNDO"])

            masters=[]
            #Detecta um master acima de cada feeder e retorna uma lista com os pares
            for cnpj in sub_df.loc[sub_df["IS_FEEDER"],"CNPJ_FUNDO_COTA"]:
                #pega a carteira do fundo investido
                temp_df=df.loc[df["CNPJ_FUNDO"]==cnpj,["VL_MERC_POS_FINAL","CNPJ_FUNDO_COTA","NM_FUNDO_COTA"]].copy()
                temp_df["PESO"]=temp_df["VL_MERC_POS_FINAL"]/pl.loc[cnpj,"VL_PATRIM_LIQ"]               #type: ignore
                #se algum o fundo investido tem algum fundo que representa mais de 90% da sua carteira, ele é um master
                master=temp_df.loc[temp_df["PESO"]>0.9,["CNPJ_FUNDO_COTA","NM_FUNDO_COTA"]]
                if not master.empty:
                    #Adiciona um item [CNPJ do feeder, CNPJ do master, nome do master]
                    masters.append([cnpj,master["CNPJ_FUNDO_COTA"].values[0],master["NM_FUNDO_COTA"].values[0]])

            #substitui os feeders pelos masters encontrados
            for master in masters:
                if sub_df.loc[sub_df["CNPJ_FUNDO_COTA"]==master[0]].empty is False:
                    sub_df.loc[sub_df["CNPJ_FUNDO_COTA"]==master[0],["CNPJ_FUNDO_COTA","NM_FUNDO_COTA","IS_FEEDER"]]=[master[1],master[2],False]
            sub_df.loc[sub_df["NM_FUNDO_COTA"].isnull(),"NM_FUNDO_COTA"]=sub_df[sub_df["NM_FUNDO_COTA"].isnull()]["CNPJ_FUNDO_COTA"]
            #Substitui sinônimos (masters com CNPJs diferentes mas que são muito parecidos)
            for s in sub_df["NM_FUNDO_COTA"]:
                if s in sinonimos.keys():
                    sub_df.loc[sub_df["NM_FUNDO_COTA"]==s,"NM_FUNDO_COTA"]=sinonimos[s]
            
            columns=[i for i in sub_df.columns if i!="PESO"]
            sub_df = sub_df.groupby(columns)['PESO'].sum().reset_index()
            
            sub_df["Gestor"]=[f[1] for _ in sub_df.index]
            #Repete o processo (caso o feeder seja feeder de um master e etc, até não conseguir mais mudar a dataframe)
            if sub_df.equals(sub_df_before):
                break

        #agrupa os fundos que no final tem o mesmo master
        summed_peso = sub_df.groupby(["Gestor","CNPJ_FUNDO_COTA","NM_FUNDO_COTA"])['PESO'].sum().reset_index()
        result=pd.concat([result,summed_peso],axis=0)

    result.reset_index(drop=True, inplace=True)
    pivot_table = result.pivot_table(index='NM_FUNDO_COTA', columns='Gestor', values='PESO', aggfunc='sum').fillna(0)
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

    fig.show()
    fig.write_image(os.path.join(".","figures",conf["filename"]+".png"))
    #print(pivot_table.loc[pivot_table["Etrnty"]>0,"Etrnty"])
    print(pivot_table.index.sort_values())

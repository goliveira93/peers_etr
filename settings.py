from datetime import datetime


#colors=["#2C4257",  
#        "#48728A",
#        "#708F92",
#        "#A3ABA4",
#        "#605869",   #cor principal  para texto de corpo
#        "#948794",
#        "#E7A75F",   #Apenas para detalhes em elementos gráficos
#        "#A25B1E"    #Apenas em gráficos
#        ]

colors=["#2C4257",    #paleta não oficial
        "#6A98B0",
        "#708F92",
        "#A3ABA4",
        "#605869",  
        "#948794",
        "#F8B865",  
        "#D3782F"            
        ]


chart_layout = dict(
    width=1280,
    height=450,
    font={"family":"Segoe UI"},
    legend={"orientation":"h"},
    xaxis= {"tickformat":",","showgrid":False, "zeroline":False},
    yaxis= {"tickformat":".2s","showgrid":False, "zeroline":False},
    margin=dict(l=20, r=20, t=25, b=25),
    hovermode = "x",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

vertical_layout = dict(
    width=1280,
    height=450,
    font={"family":"Segoe UI"},
    xaxis= {"tickformat":".1%","showgrid":False, "zeroline":False},
    yaxis= {"showgrid":False, "zeroline":False},
    margin=dict(l=20, r=20, t=20, b=20),
    hovermode = "x",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

endDate=datetime.strptime("28032024","%d%m%Y")
startDate=datetime.strptime("29022024","%d%m%Y")

fund_data = {"EVO":{
                "cod_britech": "684627",
                "fund_cnpj" : "47700200000110",
                "fund_name" : "Etrnty EVO",
                "filename1" :"47949396000181_47700200000110_ETRNTY_EVO_FIC_FIM_XML_("+startDate.strftime("%d%m%Y")+").XML",
                "filename2" :"47949396000181_47700200000110_ETRNTY_EVO_FIC_FIM_XML_("+endDate.strftime("%d%m%Y")+").XML",
                "additional_member1" : ["EVO","Ações","47700200000110"],
                "additional_member2" : ["IBX","Ações","IBX"]
             },
             "EON":{
                "cod_britech": "685038",
                "fund_cnpj" : "47716356000190",
                "fund_name" : "Etrnty ÉON",
                "filename1" :"47949396000181_47716356000190_ETRNTY_ÉON_MM_MASTER_FIC_FIM_XML_("+startDate.strftime("%d%m%Y")+").XML",
                "filename2" :"47949396000181_47716356000190_ETRNTY_ÉON_MM_MASTER_FIC_FIM_XML_("+endDate.strftime("%d%m%Y")+").XML",
                "additional_member1" : ["EON","Multimercado","47716356000190"],
                "additional_member2" : ["IFMM","Multimercado","IFMM BTG Pactual"]
             }
             }
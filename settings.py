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

fund_data = {"EVO":{
                "cod_britech": "684627",
                "fund_cnpj" : "47700200000110",
                "fund_name" : "Etrnty EVO",
                "additional_member1" : ["EVO","Ações","47700200000110"],
                "additional_member2" : ["IBX","Ações","IBX"]
             },
             "EON":{
                "cod_britech": "685038",
                "fund_cnpj" : "47716356000190",
                "fund_name" : "Etrnty ÉON",
                "additional_member1" : ["EON","Multimercado","47716356000190"],
                "additional_member2" : ["IFMM","Multimercado","IFMM BTG Pactual"]
             }
             }
import collections 
import collections.abc
from pptx import Presentation
from pptx.slide import Slide
from datetime import datetime
import os.path
from libs.delta_etrnty import gera_df
import pandas as pd
from libs.heatmap import make_heatmap
from summary import make_summary_figs
from conta_reunioes import make_numero_reunioes_fig


template_macro = 'Template.pptx'
endDate = datetime.strptime("2024-01-31","%Y-%m-%d")

def decode_layout(slide : Slide)-> tuple[list,list,list]:
    shape_dict={}
    for shape in slide.shapes:  #type: ignore
        if shape.is_placeholder:
            phf = shape.placeholder_format
            # print(f"Índice: {phf.idx}, Tipo: {phf.type}, Nome: {shape.name}")
            shape_dict[phf.idx]=(phf.type, str(phf.type))
    pics=[i for i in shape_dict if shape_dict[i][0]==18]
    bodies=[i for i in shape_dict if shape_dict[i][0]==2]
    subtitles=[i for i in shape_dict if shape_dict[i][0]==4]
    return pics,bodies,subtitles

def fill_1_grafico(slide:Slide, flat_lists:dict, i:int)->Slide:
    pics,bodies,subtitles=decode_layout(slide)
    if len(bodies)>1:
        slide.placeholders[bodies[0]].text=flat_lists["charts"][i]#type: ignore
        slide.placeholders[bodies[1]].text="gerado em "+datetime.now().strftime("%d-%b-%y")+"\n"+"com dados disponíveis até "+endDate.strftime("%d-%b-%y")  #type: ignore
    else:
        slide.placeholders[bodies[0]].text="carteiras dos concorrentes defasadas em 3 meses"  #type: ignore
    
    slide.placeholders[pics[0]].insert_picture(os.path.join(".","figures",flat_lists["files"][i]+".png"))   #type: ignore
    return slide

def fill_ciclo(slide:Slide, flat_lists:dict, i:int)->Slide:
    pics,bodies,subtitles=decode_layout(slide)
    slide.placeholders[subtitles[0]].text=flat_lists["subtitles"][i]                                    #type: ignore
    slide.placeholders[bodies[1]].text=flat_lists["charts"][i]                                          #type: ignore
    slide.placeholders[bodies[0]].text="gerado em "+datetime.now().strftime("%d-%b-%y")                 #type: ignore
    slide.placeholders[bodies[2]].text="com dados disponíveis até "+endDate.strftime("%d-%b-%y")        #type: ignore
    slide.placeholders[pics[0]].insert_picture(os.path.join(".","figures",flat_lists["files"][i]+".png"))   #type: ignore
    return slide

def fill_texto_direita(slide:Slide, flat_lists:dict, i:int)->Slide:
    pics,bodies,subtitles=decode_layout(slide)
    slide.placeholders[bodies[0]].text=flat_lists["charts"][i]                                          #type: ignore
    slide.placeholders[bodies[1]].text="gerado em "+datetime.now().strftime("%d-%b-%y")                 #type: ignore
    slide.placeholders[bodies[2]].text="com dados disponíveis até "+endDate.strftime("%d-%b-%y")        #type: ignore
    if flat_lists["files"][i] is not None:
        slide.placeholders[pics[0]].insert_picture(os.path.join(".","figures",flat_lists["files"][i]+".png"))    #type: ignore
    return slide

def fill_generic(slide:Slide)->Slide:
    pics,bodies,subtitles=decode_layout(slide)
    for e,i in enumerate(subtitles):
        slide.placeholders[i].text="subtitle idx: "+str(e)  #type: ignore
    for e,i in enumerate(bodies):
        slide.placeholders[i].text="body idx: "+str(e)      #type: ignore
    for e,i in enumerate(pics):
        slide.placeholders[i].text="pic idx: "+str(e)       #type: ignore
    return slide

def fill_2_graficos(slide: Slide, flat_lists: dict) -> Slide:
    pics, bodies, subtitles = decode_layout(slide)

    slide.placeholders[bodies[2]].text = flat_lists["charts"]["left"]  # type: ignore
    slide.placeholders[bodies[0]].text = flat_lists["charts"]["right"]  # type: ignore
    slide.placeholders[bodies[1]].text = "_contribuição do excesso de performance em relação ao fundo Etrnty"  #type: ignore
    slide.placeholders[bodies[3]].text = " "  # type: ignore

    if flat_lists["files"]["left"] is not None:
        slide.placeholders[pics[0]].insert_picture(os.path.join(".","figures", flat_lists["files"]["left"] + ".png"))  # type: ignore
    if flat_lists["files"]["right"] is not None:
        slide.placeholders[pics[1]].insert_picture(os.path.join(".","figures", flat_lists["files"]["right"] + ".png"))  # type: ignore
    if subtitles:
        slide.placeholders[subtitles[0]].text = f"Etrnty vs {flat_lists['charts']['right']}"                            #type: ignore

    return slide

def fill_performance_comp(slide, args: dict) -> Slide:
    pics, bodies, subtitles = decode_layout(slide)
    slide.placeholders[0].text=args["title"]
    if args["files"][0] is not None:
        slide.placeholders[pics[0]].insert_picture(os.path.join(".","figures", args["files"][0] + ".png"))  # type: ignore
    if args["files"][1]is not None:
        slide.placeholders[pics[1]].insert_picture(os.path.join(".","figures", args["files"][1] + ".png"))  # type: ignore
    return slide

def fill_returns(slide: Slide, df_final_ultimo_mes, gestor: str, periodo: str) -> Slide:
    # Encontra a linha no dataframe onde a coluna 'Gestor' é igual ao gestor desejado
    filtro = df_final_ultimo_mes['Gestor'] == gestor
    filtro_ETR = df_final_ultimo_mes['Gestor'] == 'Etrnty'
    
    if periodo == 'MTD':
        coluna_retorno_gestor = 'RETORNO_PEER'
        coluna_retorno_ETR = 'RETORNO_PEER'
    else:  # 'YTD'
        coluna_retorno_gestor = 'RETORNO_COMPOSTO_ACUMULADO_PEER'
        coluna_retorno_ETR = 'RETORNO_COMPOSTO_ACUMULADO_PEER'
    
    retorno_peer = df_final_ultimo_mes.loc[filtro, coluna_retorno_gestor].values[0]
    retorno_ETR = df_final_ultimo_mes.loc[filtro_ETR, coluna_retorno_ETR].values[0]

    # Formata o valor de retorno como uma string de porcentagem
    texto_retorno_peer = f"{retorno_peer:.1%}"
    texto_retorno_ETR = f"{retorno_ETR:.1%}"

    # Localiza os placeholders onde o texto deve ser inserido
    _, bodies, _ = decode_layout(slide)

    # Insere o texto no placeholder correspondente
    slide.placeholders[bodies[4]].text = texto_retorno_ETR  # type: ignore
    slide.placeholders[bodies[5]].text = texto_retorno_peer  # type: ignore    

    return slide

if __name__=="__main__":
    import sys 
    prs = Presentation(template_macro)

    #faz grafico com numero de reunioes
    make_numero_reunioes_fig(endDate)
    slide = prs.slides.add_slide(prs.slide_layouts.get_by_name("1_grafico"))
    slide.shapes.title.text = "SELEÇÃO DE GESTORES"
    fill_1_grafico(slide,{"charts":["Número de interações com gestores"],"files":["reunioes_mes"]},0)

    #faz graficos de barra com performance absoluta YTD, MTD para eon e evo
    try:
        make_summary_figs(endDate)
    except ValueError as e:
        print(e)
        pass
    layouts = {"1_grafico": fill_1_grafico, "ciclo": fill_ciclo, "2_graficos": fill_2_graficos, "texto_direita": fill_texto_direita, "comps_slide": fill_1_grafico}
    gestores = ["Brain", "Consenso", "Etrnty", "G5", "JBFO", "Mandatto", "Portofino", "Pragma", "Taler", "Vitra", "Warren", "Wright", "XPA"]

    print("Gerando gráficos de comparação")
    for fund, tipo, prefix, slide_top_color in zip(["EVO","EON"],["Ações","Multimercado"],["FIA","FIM"],["gray","blue"]):
        #Pega .png dos gráficos de barra (performance absoluta) e coloca no pptx
        slide = prs.slides.add_slide(prs.slide_layouts.get_by_name("performance_comp_"+slide_top_color))
        fill_performance_comp(slide,{"files":[fund.lower()+"_MTD",fund.lower()+"_YTD"],"title":"ETRNTY "+fund})

        #Gera heatimap com posições dos concorrentes
        df=gera_df(fund,"MTD",False)
        df=df[df["Gestor"].isin(gestores)]
        make_heatmap(fund,df)
        slide = prs.slides.add_slide(prs.slide_layouts.get_by_name("comps_slide_"+slide_top_color))
        slide.shapes.title.text = "PEERS - "+fund
        fill_1_grafico(slide,{"charts":["carteira pares"],"files":["heatmap_"+fund]},0)

        #gera comparações
        for period in ["MTD","YTD"]:
            try:
                df_final_ultimo_mes=gera_df(fund,period)
            except Exception as e:
                print("Erro gerando gráficos para: "+fund)
                break

            for gestor in gestores:
                if gestor != "Etrnty":
                    # Define os nomes dos arquivos para MTD e YTD
                    arquivo_gestor = prefix+"_"+gestor+"_"+period
                    arquivo_etrnty = prefix+"_Etrnty"+"_"+period
                    
                    # Adiciona um novo slide para MTD
                    slide = prs.slides.add_slide(prs.slide_layouts.get_by_name("2_graficos"))
                    # Preencher o slide com dados de MTD
                    slide = fill_returns(slide, df_final_ultimo_mes, gestor, period)
                    # Configurar títulos, gráficos, e outras informações para MTD e YTD
                    slide.shapes.title.text = tipo+" - "+period                                #type: ignore
                    
                    # Preencher os slides com os dados
                    flat_lists = {
                        "charts": {"left": "Etrnty", "right": gestor},
                        "files": {"left": arquivo_etrnty, "right": arquivo_gestor}
                    }
                    
                    # Preencher o slide com layouts específicos
                    layouts["2_graficos"](slide, flat_lists)

            # Salvar a apresentação
    filename = "Comparacao_"
    prs.save(os.path.join("PPT", filename + endDate.strftime("%m-%y") + ".pptx"))
    print("Apresentação salva como " + filename + endDate.strftime("%m-%y") + ".pptx")


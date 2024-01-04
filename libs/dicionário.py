import os
import pandas as pd

PEERS = [["26.470.647/0001-70", "Mandatto"],
         ["13.958.690/0001-38", "Taler"],
         ["07.875.686/0001-03", "Consenso"],
         ["37.703.536/0001-83", "Warren"],
         ["34.633.416/0001-69", "Vitra"],
         ["06.128.183/0001-01", "JBFO"],
         ["24.371.991/0001-87", "Wright"],
         ["11.389.643/0001-95", "Pragma"],
         ["35.610.530/0001-36", "Brain"],
         ["18.060.935/0001-29", "G5"],
         ["25.098.042/0001-38", "XPA"],
         ["47.700.200/0001-10", "Etrnty"],
         ["26.470.596/0001-87", "Mandatto"],
         ["07.382.415/0001-16", "Taler"],
         ["20.969.330/0001-05", "Consenso"],
         ["46.615.722/0001-51", "Warren"],
         ["34.633.424/0001-05", "Vitra"],
         ["05.778.214/0001-07", "JBFO"],
         ["22.884.922/0001-41", "Wright"],
         ["11.389.633/0001-50", "Pragma"],
         ["34.617.263/0001-66", "Brain"],
         ["04.869.180/0001-01", "G5"],
         ["28.777.487/0001-32", "XPA"],
         # ["32.254.387/0001-07","São João"],
         ["47.716.356/0001-90", "Etrnty"],
         ["36.727.650/0001-80", "Portofino"],
         ["37.227.781/0001-61", "Portofino"]]

data_dir = os.path.join(".", "tools", "dataset_cvm", "data")
all_masters_list = []

base_pl_file = "cda_fi_PL_2023{}.csv"
base_blc_file = "cda_fi_BLC_2_2023{}.csv"

for month in range(8, 9):  # Altere para range(1, 12) se você tiver dados para todos os meses
    month_str = f"{month:02d}"
    pl_file = base_pl_file.format(month_str)
    blc_file = base_blc_file.format(month_str)

    pl_path = os.path.join(data_dir, pl_file)
    blc_path = os.path.join(data_dir, blc_file)

    if not os.path.exists(pl_path) or not os.path.exists(blc_path):
        print(f"Arquivo para o mês {month_str} não existe.")
        continue

    pl = pd.read_csv(pl_path, delimiter=";", encoding="ISO-8859-1")
    df = pd.read_csv(blc_path, delimiter=";", encoding="ISO-8859-1")

    pl = pl[["CNPJ_FUNDO", "VL_PATRIM_LIQ"]].set_index("CNPJ_FUNDO")
    df = df[["TP_FUNDO", "CNPJ_FUNDO", "DENOM_SOCIAL", "TP_ATIVO",
             "VL_MERC_POS_FINAL", "CNPJ_FUNDO_COTA", "NM_FUNDO_COTA"]]

    for peer in PEERS:
        cnpj_fundo = peer[0]
        sub_df = df[df["CNPJ_FUNDO"] == cnpj_fundo]

        if sub_df.empty:
            # Se sub_df está vazio, significa que não encontramos o fundo e devemos adicioná-lo como desconhecido
            # Verificamos se o CNPJ_FUNDO_COTA já foi adicionado para evitar duplicidade
            if not any(d['CNPJ_FUNDO_COTA'] == cnpj_fundo for d in all_masters_list):
                all_masters_list.append({
                    "CNPJ_FUNDO_COTA": cnpj_fundo,
                    "FUNDO_NOME": "Desconhecido",
                    "CNPJ_MASTER": "Desconhecido",
                    "NOME_MASTER": "Desconhecido",
                })
            continue

        # Se não for vazio, processamos cada fundo
        for _, fundo in sub_df.iterrows():
            cnpj_cota = fundo["CNPJ_FUNDO_COTA"] if pd.notnull(
                fundo["CNPJ_FUNDO_COTA"]) else cnpj_fundo
            nome_feeder = fundo["NM_FUNDO_COTA"] if pd.notnull(
                fundo["NM_FUNDO_COTA"]) else "Desconhecido"

            if cnpj_cota in pl.index:
                temp_df = df[df["CNPJ_FUNDO"] == cnpj_cota].copy()
                temp_df["PESO"] = temp_df["VL_MERC_POS_FINAL"] / \
                    float(pl.loc[cnpj_cota, "VL_PATRIM_LIQ"])
                master = temp_df[temp_df["PESO"] > 0.9]

                if not master.empty:
                    # Aqui assumimos que o maior peso indica o master
                    master_cnpj = master.iloc[0]["CNPJ_FUNDO_COTA"]
                    master_nome = master.iloc[0]["NM_FUNDO_COTA"]
                else:
                    master_cnpj = "Desconhecido"
                    master_nome = "Desconhecido"
            else:
                print(f"CNPJ {cnpj_cota} não encontrado no arquivo PL.")
                master_cnpj = "Desconhecido"
                master_nome = "Desconhecido"

            # Adiciona ao all_masters_list apenas se o CNPJ_FUNDO_COTA ainda não foi inserido
            if not any(d['CNPJ_FUNDO_COTA'] == cnpj_cota for d in all_masters_list):
                all_masters_list.append({
                    "CNPJ_FUNDO_COTA": cnpj_cota,
                    "FUNDO_NOME": nome_feeder,
                    "CNPJ_MASTER": master_cnpj,
                    "NOME_MASTER": master_nome,
                })


final_masters_df = pd.DataFrame(all_masters_list).drop_duplicates()
final_masters_df.to_excel("final_masters_funds.xlsx", index=False)

print("O arquivo 'final_masters_funds.xlsx' foi criado com sucesso!")

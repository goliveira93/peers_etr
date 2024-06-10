"""
Cria conexão com o banco de dados e a variável de sessão: CVM_datafeed_session
"""
# pylint: disable=invalid-name
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, Integer, String, Date, Float, func
from sqlalchemy.orm import sessionmaker, mapped_column
import pandas as pd

__cvm_datafeed_engine__  = create_engine("mysql+mysqldb://"+"cvm_datafeed"+":"+"cvm_123"+"@172.16.215.2/"+"cvm_datafeed")
__cvm_datafeed_base__ = declarative_base()
__cvm_datafeed_base__.metadata.create_all(__cvm_datafeed_engine__)
CVM_datafeed_session= sessionmaker(bind=__cvm_datafeed_engine__)


class Fundos_sinonimos(__cvm_datafeed_base__):
    """
    Classe que define a tabela fundos_sinonimos
    """
    __tablename__="fundos_sinonimos"
    ID=mapped_column(Integer,primary_key=True)
    fundo_nome=mapped_column(String)
    nome_master=mapped_column(String)
    cnpj_fundo_cota=mapped_column(String(20))
    sinonimo=mapped_column(String)


class CVM_peers(__cvm_datafeed_base__):
    """
    Classe geral para tabelas de peers, a ser morfada por classes específicas depois
    """
    __tablename__   ="cvm_peers"
    id              =mapped_column(Integer, primary_key=True)
    tipo            =mapped_column(String)
    gestor          =mapped_column(String)
    dt_comptc       =mapped_column(Date)
    cnpj_fundo      =mapped_column(String)
    cnpj_fundo_cota =mapped_column(String)
    nm_fundo_cota   =mapped_column(String)
    peso            =mapped_column(Float)
    retorno         =mapped_column(Float)
    retorno_peer    =mapped_column(Float)
    contribuicao    =mapped_column(Float)


def upload_to_db(list_of_pairs):
    """
    Envia lista de fundos para o banco de dados. Fundos repetidos são ignorados
    """
    session = CVM_datafeed_session()
    for pair in list_of_pairs:
        fundo_nome = pair[0]
        sinonimo = pair[1]

        # Check if fundo_nome already exists
        exists = session.query(Fundos_sinonimos).filter_by(fundo_nome=fundo_nome).first()
        if not exists:
            new_entry = Fundos_sinonimos(fundo_nome=fundo_nome, sinonimo=sinonimo)
            session.add(new_entry)

    session.commit()
    session.close()

def fetch_all_fundos():
    """
    Pega todos os fundos da tabela
    """
    # Start a new session
    session = CVM_datafeed_session()
    # Query all the entries in the fundos_sinonimos table
    results = session.query(Fundos_sinonimos).all()

    # Convert each entry to a two-item list
    fundos_list = [[entry.fundo_nome, entry.sinonimo] for entry in results]
    fundos_list +=[[entry.nome_master, entry.sinonimo] for entry in results]
    fundos_list +=[[entry.cnpj_fundo_cota, entry.sinonimo] for entry in results]
    fundos_dict={i[0]:i[1] for i in fundos_list}

    # Close the session
    session.close()

    return fundos_dict


def fetch_all_fundos_list():
    """
    Pega todos os fundos da tabela
    """
    # Start a new session
    session = CVM_datafeed_session()
    # Query all the entries in the fundos_sinonimos table
    results = session.query(Fundos_sinonimos).all()

    # Convert each entry to a two-item list
    fundos_list = [[entry.fundo_nome, entry.sinonimo] for entry in results]
    fundos_list +=[[entry.nome_master, entry.sinonimo] for entry in results]
    fundos_list +=[[entry.cnpj_fundo_cota, entry.sinonimo] for entry in results]
    fundos_dict={i[0]:i[1] for i in fundos_list}

    # Close the session
    session.close()

    return fundos_list

def fetch_arquivo_cmv(file_date:datetime)->pd.DataFrame:
    with CVM_datafeed_session() as session:
        # Query all the entries in the fundos_sinonimos table
        results = session.query(CVM_peers).filter(CVM_peers.dt_comptc==file_date)
        # Fetching results and converting to a list of dictionaries
        data = [row.__dict__ for row in results]

        # Remove SQLAlchemy's internal attribute '_sa_instance_state'
        for row in data:
            row.pop('_sa_instance_state', None)

        # Create a pandas DataFrame
        df = pd.DataFrame(data)
    return df

def get_fund_return(NM_FUNDO_COTA:str, start_date:datetime, end_date:datetime)->float:
    with CVM_datafeed_session() as session:
        results = session.query(
            CVM_peers.dt_comptc,
            func.avg(CVM_peers.retorno).label('mean_retorno')
        ).filter(
            CVM_peers.nm_fundo_cota == NM_FUNDO_COTA,
            CVM_peers.dt_comptc >= start_date,
            CVM_peers.dt_comptc <= end_date  # Assuming you have an end_date variable
        ).group_by(
            CVM_peers.dt_comptc
        ).order_by(
            CVM_peers.dt_comptc
        ).all()
        ret=0
        for r in results:
            if r.mean_retorno is None:
                raise ValueError("Não foi possível encontrar retorno para o fundo: "+NM_FUNDO_COTA+" ("+start_date.strftime("%Y-%m-%d")+" : "+end_date.strftime("%Y-%m-%d")+")")
            ret=(1+ret)*(1+r.mean_retorno)-1
    return ret

if __name__=="__main__":
    #l=fetch_arquivo_cmv(datetime.strptime("2023-06-30","%Y-%m-%d"))
    #print(l)
    print(get_fund_return("Núcleo",datetime(2023,12,29),datetime(2024,2,29)))

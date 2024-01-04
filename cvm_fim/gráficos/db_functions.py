"""
Cria conexão com o banco de dados e a variável de sessão: CVM_datafeed_session
"""
# pylint: disable=invalid-name

from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, Integer, String, Date, Float
from sqlalchemy.orm import sessionmaker, mapped_column

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


__cvm_datafeed_base__.metadata.create_all(__cvm_datafeed_engine__)
CVM_datafeed_session= sessionmaker(bind=__cvm_datafeed_engine__)


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
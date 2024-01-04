"""
Cria conexão com o banco de dados e a variável de sessão: CVM_datafeed_session
"""
# pylint: disable=invalid-name

from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, Integer, String
from sqlalchemy.orm import sessionmaker, mapped_column

__cvm_datafeed_engine__  = create_engine("mysql+mysqldb://"+"cvm_datafeed"+":"+"cvm_123"+"@172.16.215.2/"+"cvm_datafeed")
__cvm_datafeed_base__ = declarative_base()

class Fundos_sinonimos(__cvm_datafeed_base__):
    """
    Classe que define a tabela fundos_sinonimos
    """
    __tablename__="fundos_sinonimos"
    ID=mapped_column(Integer,primary_key=True)
    fundo_nome=mapped_column(String)
    sinonimo=mapped_column(String)

__cvm_datafeed_base__.metadata.create_all(__cvm_datafeed_engine__)
CVM_datafeed_session= sessionmaker(bind=__cvm_datafeed_engine__)


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

    # Close the session
    session.close()

    return fundos_list
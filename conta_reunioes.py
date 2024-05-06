import os
import calendar
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, String, SmallInteger, Float, Boolean, Date, Text, LargeBinary
from sqlalchemy.orm import sessionmaker, relationship, mapped_column, declarative_base
import plotly.graph_objects as go
from summary import colors #type: ignore

chart_horizontal_layout = dict(
    width=1500,
    height=500,
    margin={"l":100,"r":20},
    font={"family":"Segoe UI","size":15},
    legend={"yanchor":"bottom",
            "xanchor":"left",
            "x":0,
            "y":-0.2,
            "orientation":"h"},
    #shapes=recs,
    yaxis={
        "tickformat":","
        },
    xaxis={
        "domain": [0, 0.97]  # Set the domain of the x-axis to use 90% of the available space
    },
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)

__estrela_vespertina_base__ = declarative_base()

class Conta_reunioes(__estrela_vespertina_base__):
    __tablename__="conta_reunioes"
    id = mapped_column(Integer, primary_key=True)
    data=mapped_column(Date)
    numero_reunioes = mapped_column(Integer)

def make_numero_reunioes_fig(data_final:datetime)->go.Figure:
    """
    Faz gráfico de barras com numero de reunioes por mês
    """
    print("Calculando número de reuniões/mês")
    user = os.getenv("user")
    password = os.getenv("password")
    host = os.getenv("host")
    port = os.getenv("port")
    database = "estrela_vespertina"

    ultimo_dia=calendar.monthrange(data_final.year, data_final.month)[1]   #ultimo dia corrido do mês
    data_final=datetime(data_final.year,data_final.month,ultimo_dia)
    __estrela_vespertina_engine__  = create_engine("mysql+mysqldb://"+user+":"+password+"@"+host+"/"+database) #type: ignore
    __estrela_vespertina_base__.metadata.create_all(__estrela_vespertina_engine__)
    Estrela_vespertina_session= sessionmaker(bind=__estrela_vespertina_engine__)

    with Estrela_vespertina_session() as session:
        result = session.query(Conta_reunioes.data, Conta_reunioes.numero_reunioes).filter(Conta_reunioes.data>datetime.strptime("30-06-2023","%d-%m-%Y")).filter(Conta_reunioes.data<=data_final).all()
    dates = [datetime.strftime(d[0],"%b-%y") for d in result]
    values = [d[1] for d in result]
    range=[0,max(values)*1.1]

    fig = go.Figure(data=go.Bar(x=dates, y=values, marker_color=colors[0], text=[str(i) for i in values], textposition='outside'))

    # Adding chart layout details
    fig.update_layout(
        chart_horizontal_layout,
        yaxis={"range":range},
        yaxis_title='número de reuniões',
        bargap=0.2  # control the space between bars
    )

    # Displaying the plot
    fig.write_image(os.path.join(".","figures","reunioes_mes.png"))
    return fig

if __name__=="__main__":
    import sys
    make_numero_reunioes_fig(datetime.today())
    sys.exit()

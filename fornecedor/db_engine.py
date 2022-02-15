import configparser

from sqlalchemy import create_engine
from urllib.parse import quote

##Teste
def dbEngine(session):
    # Cria uma engine, objeto necessário para manipular objetos no banco de dados. A engine é a ponte entre a aplicação e o DB.
    # session refere-se a sessão do arquivo de configuração carregado no metodo .read()

    config = configparser.ConfigParser()
    config.read('config/sql_config.cfg')
    
    SESSION = session
    
    # Connection example
    # dialect+driver://username:password@host:port/DATAGEO
    
    engine = create_engine('mssql+pymssql://{}:{}@{}:{}/{}'.format(quote(config[SESSION]['user']),quote(config[SESSION]['pass']), config[SESSION]['host'], config[SESSION]['port'], config[SESSION]['db']))

    return engine

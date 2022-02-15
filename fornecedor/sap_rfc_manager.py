from pyrfc import Connection
import configparser
import pandas as pd


def sapConfigConnection():
    # Configuração dos parametros do arquivo de configuração de conexão com o SAP.
        
    config = configparser.ConfigParser()
    config.read("config/sap_nw_rfc.cfg")
    params_connection = config._sections["connection"]

    return Connection(**params_connection)



def getRemoveCols(source_cols, new_cols): 
        # Verifica quais colunas da origem não estão no novo esquema, classifica e retorna a lista dessas para serem excluidas.

        cols = []

        for x in source_cols.keys():
                if not x in new_cols.keys():
                        cols.append(x)

        return cols


def sapRFCManager(rfc_name, dir_temp, filename, cols, **kwargs):
    # Função responsável por gerenciar chamadas via RFC no SAP.
    #
    # dir_temp   -> Diretório temporario para armazenar as saídas.
    # filename   -> Arquivo a ser gerado após a chamada da função.
    # new_schema -> Informar o nome das colunas a serem renomeadas, caso o parametro seja vazio sera mantido os nomes vindos do SAP.
    

    conn = sapConfigConnection()

    result = conn.call(rfc_name, **kwargs)

    columns = []
    data = []

    for e in result.values():
        # Itera uma vez no data frame recuperando o nome das colunas
        for i in range(len(e)):
            columns = list(e[i].keys())
            break

    for e in result.values():
        # Itera no data frame recuperando os valores
        for i in range(len(e)):
            data.append(list(e[i].values()))

    df = pd.DataFrame(data = data, columns = columns)
    
    
    df = df.drop(columns = getRemoveCols(df, cols))

    df = df.rename(columns = cols)

    df.to_csv(path_or_buf = dir_temp +'/'+ filename + ".csv", header = True, sep= ";", index = False, encoding = 'utf-8')
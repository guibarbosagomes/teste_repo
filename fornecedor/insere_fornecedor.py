import datetime
import pandas as pd
from sqlalchemy import engine
from sqlalchemy.types import Integer, String, Date
from sap_rfc_manager import sapRFCManager
from new_rows import newRows
from db_engine import dbEngine

sap_param = {
        'DATE_LOW': datetime.datetime.strptime('2011-01-01', '%Y-%m-%d').date(), 
        'DATE_HIGH': datetime.datetime.strptime('2040-01-01', '%Y-%m-%d').date()
    }


col_fornecedores = {
    'LIFNR' : 'cod_fornecedor',
    'LAND1' : 'pais',
    'NAME1' : 'nome',
    'NAME2' : 'nome2',
    'ORT01' : 'local',
    'ORT02' : 'cidade',
    'PSTLZ' : 'codigo_postal',
    'REGIO' : 'regiao',
    'STRAS' : 'rua_numero',
    'ADRNR' : 'endereco',
    'ANRED' : 'forma_tratamento',
    'ERDAT' : 'dt_criacao',
    'ERNAM' : 'responsavel',
    'KTOKK' : 'grupo_contas',
    'STCD1' : 'cnpj',
    'STCD2' : 'cpf',
    'TELF1' : 'telefone1',
    'TELF2' : 'telefone1',
    'TXJCD' : 'domicilio_fiscal',
    'STCD3' : 'inscricao_estadual',
    'STCD4' : 'rg',
    'BANKS' : 'cod_pais_banco',
    'BANKL' : 'agencia',
    'BANKN' : 'conta_bancaria',
    'BKONT' : 'chave_bancos',
    'KOINH' : 'titular_conta_bancaria',
}

column_types = {
    'cod_fornecedor' : String(20),
    'pais' : String(10),
    'nome' : String(100),
    'nome2' : String(50),
    'local' : String(50),
    'cidade' : String(50),
    'codigo_postal' : String(50),
    'regiao' : String(50),
    'rua_numero' : String(50),
    'endereco' : String(50),
    'forma_tratamento' : String(50),
    'dt_criacao' : Date(),
    'responsavel' : String(50),
    'grupo_contas' : String(50),
    'cnpj' : String(20),
    'cpf' : String(50),
    'telefone1' : String(20),
    'telefone2' : String(20),
    'domicilio_fiscal' : String(50),
    'inscricao_estadual' : String(50),
    'rg' : String(20),
    'cod_pais_banco' : String(20),
    'agencia' : String(20),
    'conta_bancaria' : String(20),
    'chave_bancos' : String(20),
    'titular_conta_bancaria' : String(100),
}

# sapRFCManager('ZBO_DADOS_MESTRE_FORNE', dir_temp= 'temp', filename='fornecedor', cols = col_fornecedores, **sap_param)
forn_csv = pd.read_csv('temp/fornecedor.csv', sep=';', dtype='object')

# ordena o data frame
forn_csv = forn_csv.sort_values(by='cod_fornecedor', ascending= True)

## Remove os 0 a esquerda da chave
forn_csv['cod_fornecedor'] = forn_csv['cod_fornecedor'].str.lstrip('0')

## Atualiza os novos registros no banco !
newRows(pd.read_sql('fornecedor', dbEngine('DATAGEO')), forn_csv, 'cod_fornecedor').to_sql('fornecedor', con = dbEngine('DATAGEO'), index= False, if_exists='append', dtype=column_types)





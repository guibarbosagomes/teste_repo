import pandas as pd


def newRows(df1, df2, key):
    ## Compara dois dataframes a partir de uma chave e retorna as linhas que não estão no primeiro, ou seja, novas linhas.
    
    df3 = pd.merge(df1.sort_values([key]),df2.sort_values([key]), on=key, how='right', indicator = True)
    
    df3 = df3[df3['_merge'] == 'right_only']

    ## Seleciona todas as colunas que serão inseridas a patrir do index gerado pelo pandas, neste caso por default _y
    df3 = df3[(col for col in df3.columns if col[-2:] == '_y' or col == key)]

    ## Apos selecionar as colunas pelo index _y aqui as mesmas são renomeadas, removendo o _y
    for col in df3.columns:
        df3.rename(columns = { col : col.replace('_y','')}, inplace= True)
    
    return df3
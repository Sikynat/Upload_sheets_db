
import mysql.connector
import pandas as pd
import numpy as np

try:
    conexao = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="prototype"
    )
    cursor = conexao.cursor()

    # --- 1. Ler as duas planilhas ---
    # Lendo o arquivo de ES e renomeando as colunas
    df_es = pd.read_excel(r'ProdutosDia/table-price-ES.xlsx', usecols=['CÓDIGO', 'DESCRIÇÃO', 'GRUPO', 'TABELA'])
    df_es = df_es.rename(columns={'CÓDIGO': 'cod_product', 'DESCRIÇÃO': 'desc_product', 'GRUPO': 'group_product', 'TABELA': 'value_product_ES'})

    # Lendo o arquivo de SP e renomeando a coluna
    df_sp = pd.read_excel(r'ProdutosDia/table-price-ES.xlsx', usecols=['CÓDIGO', 'TABELA'])
    df_sp = df_sp.rename(columns={'CÓDIGO': 'cod_product', 'TABELA': 'value_product_SP'})
    
    # --- 2. Juntar os DataFrames ---
    # O pd.merge() junta os dados com base na coluna 'cod_product'
    df_final = pd.merge(df_es, df_sp, on='cod_product', how='left')
    
    # --- 3. Tratar e Preparar os dados ---
    # Garante que os valores NaN sejam tratados como None
    df_final = df_final.replace({np.nan: None})

    # Pega apenas as colunas que serão inseridas no banco de dados
    df_para_inserir = df_final[['cod_product', 'desc_product', 'group_product', 'value_product_SP', 'value_product_ES']]

    # Converte o DataFrame para uma lista de tuplas
    dados_para_inserir = [tuple(row) for row in df_para_inserir.values]
    
    # --- 4. Executar a inserção ---
    # A query com 5 placeholders, que correspondem às colunas do DataFrame
    query = "INSERT INTO products (cod_product, desc_product, group_product, value_product_SP, value_product_ES, date_product) VALUES (%s, %s, %s, %s, %s, CURDATE())"

    # Um único executemany para todos os dados combinados
    cursor.executemany(query, dados_para_inserir)

    conexao.commit()
    print(f"{cursor.rowcount} linhas inseridas na tabela 'products' com sucesso.")

except mysql.connector.Error as erro:
    print(f"Erro ao inserir dados no MySQL: {erro}")

finally:
    if 'conexao' in locals() and conexao.is_connected():
        cursor.close()
        conexao.close()
        print("Conexão com o MySQL fechada.")

        input('Pressione qualquer tecla para continuar ')
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

    # Leitura do arquivo Excel
    tabela_produtos = pd.read_excel(r'upClientes/tabela_clientes.xlsx', usecols=['Status', 'Código Wefix', 'Nome', 'Endereço', 'UF', 'Município', 'Contato','Nota', 'Frete'])

    # TRATAMENTO DE VALORES NULOS (NaN)
    tabela_produtos = tabela_produtos.replace({np.nan: None})

    # Preparação dos dados para o executemany
    dados_para_inserir = [tuple(row) for row in tabela_produtos.values]
    
    # A query de INSERT, com placeholders (%s)
    query = "INSERT INTO clients (client_status, client_code, client_name, client_adress, client_uf, client_city, client_contact, client_nf, client_frete, client_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURDATE())"



    # Executa a inserção em lote com a lista de tuplas corrigida
    cursor.executemany(query, dados_para_inserir)

    # Confirma as alterações no banco de dados
    conexao.commit()
    print(f"{cursor.rowcount} linhas inseridas com sucesso.")

   
except mysql.connector.Error as erro:
    print(f"Erro ao inserir dados no MySQL: {erro}")

finally:
    if 'conexao' in locals() and conexao.is_connected():
        cursor.close()
        conexao.close()
        print("Conexão com o MySQL fechada.")
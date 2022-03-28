# Importando Bibliotecas
from datetime import datetime
import sqlalchemy 
import psycopg2


# Importando as funcoes do arquivo functions.py
from move_files.meu_modulo.minhas_funcoes import (get_data, 
                                                  sep_empresas, 
                                                  define_dataframe, 
                                                  add_resolucao, 
                                                  exporta_dataframe, 
                                                  get_links,
                                                  concatena_arquivos,
                                                  dataframe_categorias,
                                                  dataframe_assuntos,
                                                  dataframe_principal,
                                                  move_files,
                                                  envia_email, log)

def etl_principal():
    log('Iniciando Processo')
    def get_data_from_all_links(link_lista):
        # Iterando os links da lista
        for link in link_lista:
            log(f'Iniciando ETL da resolução {link}')
            # recebendo os dados da pagina referente ao link
            # Etapa de Extração
            dados = get_data(link)
            log('Dados Coletados')
            # Etapa de Transformação dos dados
            dados_tratados = sep_empresas(dados)
            log('Dados Tratados')
            dataframe = define_dataframe(dados_tratados)
            df_final = add_resolucao(dados, dataframe)
            log('Dataframe Finalizado')
            # Etapa de Load
            exporta_dataframe(df_final, '/mnt/c/airflow/dags/move_files/resolucoes/Resolucao')
            log('Arquivo exportado')
            log('ETL Finalizado')
            
    # Utiliza a Biblioteca Selenium para obter o link de todas as resolucoes do periodo determinados
    log('Iniciando Coleta de Links')       
    lista_links = get_links()
    print(lista_links)
    log('Links Coletados')

    # Chama a função que realiza o processo principal de ETL dos dados 
    log('Iniciando Iteração de links')      
    get_data_from_all_links(lista_links)
    log('Iteração realizada com sucesso! Todos os dados foram coletados, tratados e exportados!')
    

def etl_secundario():
    # Gera um unico arquivo a partir dos arquivos gerados anteriormente e exporta no formato requisitado pelo cliente
    log('Iniciando Concatenação dos arquivos')
    df_completo = concatena_arquivos()
    log('Arquivo final com todos os dados referentes ao periodo selecionado, foi gerado com sucesso!')
    
    # Gera o dataframe referente as categorias
    log('Gerando dataframe categorias!')
    df_categorias = dataframe_categorias(df_completo)
    exporta_dataframe(df_categorias, '/mnt/c/airflow/dags/move_files/database_backup/Categorias')
    log('Dataframe gerado e exportado com sucesso!')
    print(df_categorias)
    
    # Gera o dataframe referente aos assuntos
    log('Gerando dataframe assuntos!')
    df_assuntos = dataframe_assuntos(df_completo)
    exporta_dataframe(df_assuntos, '/mnt/c/airflow/dags/move_files/database_backup/Assuntos')
    log('Dataframe gerado e exportado com sucesso!')
    print(df_assuntos)

    # Gera o dataframe saneantes
    log('Gerando dataframe saneantes!')
    df_saneantes = dataframe_principal(df_completo)
    exporta_dataframe(df_saneantes, '/mnt/c/airflow/dags/move_files/database_backup/Saneantes')
    log('Dataframe gerado e exportado com sucesso!')
    print(df_saneantes)
    
    # Carregando os dados para o banco de dados
    # Criando conexão com o banco de dados
    database = 'postgresql://postgres:password@localhost:5432/move' 
    engine = sqlalchemy.create_engine(database)
    conn = psycopg2.connect(database="move", user="postgres", password="password")
    
    log("Database aberto com sucesso!")
    
    """Esta não é a forma mais eficiente de fazer isso, mas é a forma que funcionou para o momento, uma vez que o codigo para
    criar uma tabela temporaria para inserir os dados do dataframe e usar essa tabela para inserir os dados na tabela de produçao
    onde os valores de id não exixtem, por algum motivo que eu não consegui identificar não estava funcionando. Mas vou continuar investigando
    para fazer funcionar, uma vez que funciona para outros projetos."""
    try:
        df_categorias.to_sql('categoria', engine, index=False, if_exists="append")
        log("Dados de categoria inseridos com sucesso")
    except:
        log("Dados já existentes no banco de dados!")
    
    try:
        df_assuntos.to_sql('assunto', engine, index=False, if_exists="append")
        log("Dados de assunto inseridos com sucesso")
    except:
        log("Dados já existentes no banco de dados!")
    
    # Como nenhum dados deste é unico, os dados podem ser inseridos no banco de dados sem problemas
    df_saneantes.to_sql('saneantes', engine, index=False, if_exists="append")
    log("Dados de saneantes inseridos com sucesso")
    
    conn.close()
    
def envia_email_e_move_arquivos():
    # Envia um email para o cliente com o arquivo gerados
    log('Enviando email com para o cliente com o arquivo gerado!')
    envia_email('email_remetente', 'email_destinatario', 'senha_do_remetente')
    log('Email enviado com sucesso!')
    
    # Move os arquivo lidos para uma pasta de backup
    log('Movendo os arquivos csv para uma pasta de backup')
    move_files('/mnt/c/airflow/dags/move_files/resolucoes', '/mnt/c/airflow/dags/move_files/resolucoes_backup')
    log('Arquivos movidos com sucesso!')
    
    # Move os arquivo lidos para uma pasta de backup
    log('Movendo o arquivo xlsx para uma pasta de backup')
    move_files('/mnt/c/airflow/dags/move_files/cliente', '/mnt/c/airflow/dags/move_files/cliente_backup')
    log('Arquivo movido com sucesso!')
    log("Processo finalizado com sucesso!")
    

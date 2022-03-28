# Importando Bibliotecas
from datetime import datetime
import sqlalchemy 
import psycopg2

# Importando as funcoes do arquivo functions.py
from funcoes import (get_data, 
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

start = datetime.now()
log('Iniciando Processo')
# Definindo uma função para rodar o pipeline
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
        exporta_dataframe(df_final, 'resolucoes\Resolucao')
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

# Gera um unico arquivo a partir dos arquivos gerados anteriormente e exporta no formato requisitado pelo cliente
log('Iniciando Concatenação dos arquivos')
df_completo = concatena_arquivos()
log('Arquivo final com todos os dados referentes ao periodo selecionado, foi gerado com sucesso!')

# Move os arquivo lidos para uma pasta de backup
log('Movendo os arquivos csv para uma pasta de backup')
move_files(r'resolucoes', r'resolucoes_backup')
log('Arquivos movidos com sucesso!')

# Envia um email para o cliente com o arquivo gerados
# log('Enviando email com para o cliente com o arquivo gerado!')
# envia_email('costa.camila.ro@gmail.com', 'lila.costa.ro@gmail.com', 'Tom2910#')
# log('Email enviado com sucesso!')

# Move os arquivo lidos para uma pasta de backup
log('Movendo o arquivo xlsx para uma pasta de backup')
move_files(r'cliente', r'cliente_backup')
log('Arquivo movido com sucesso!')

# Gera o dataframe referente as categorias
log('Gerando dataframe categorias!')
df_categorias = dataframe_categorias(df_completo)
exporta_dataframe(df_categorias, 'to_database\categoria\Categorias')
log('Dataframe gerado e exportado com sucesso!')
print(df_categorias)

# Gera o dataframe referente aos assuntos
log('Gerando dataframe assuntos!')
df_assuntos = dataframe_assuntos(df_completo)
exporta_dataframe(df_assuntos, 'to_database\Assunto\Assuntos')
log('Dataframe gerado e exportado com sucesso!')
print(df_assuntos)

# Gera o dataframe saneantes
log('Gerando dataframe saneantes!')
df_saneantes = dataframe_principal(df_completo)
exporta_dataframe(df_saneantes, 'to_database\saneantes\Saneantes')
log('Dataframe gerado e exportado com sucesso!')
print(df_saneantes)


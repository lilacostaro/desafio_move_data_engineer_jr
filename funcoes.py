import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pytz
import glob 
from selenium import webdriver
from time import sleep
import os
import shutil
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def get_links():
    data = datetime.now()
    mes = data.strftime('%b')
    year = int(data.strftime('%Y'))
    mes = 'Feb'
    year = 2021
    
    if mes == 'Jan':
        data_inicial = f'01-12-{year - 1}'
        data_final = f'31-12-{year - 1}'
    elif mes == 'Feb':
        data_inicial = f'01-01-{year}'
        data_final = f'31-01-{year}'
    elif mes == 'Mar':
        data_inicial = f'01-02-{year}'
        data_final = f'28-02-{year}'
    elif mes == 'Apr':
        data_inicial = f'01-03-{year}'
        data_final = f'31-03-{year}'
    elif mes == 'May':
        data_inicial = f'01-04-{year}'
        data_final = f'30-04-{year}'
    elif mes == 'Jun':
        data_inicial = f'01-05-{year}'
        data_final = f'31-05-{year}'
    elif mes == 'Jul':
        data_inicial = f'01-06-{year}'
        data_final = f'30-06-{year}'
    elif mes == 'Aug':
        data_inicial = f'01-07-{year}'
        data_final = f'31-07-{year}'
    elif mes == 'Sep':
        data_inicial = f'01-08-{year}'
        data_final = f'31-08-{year}'
    elif mes == 'Oct':
        data_inicial = f'01-09-{year}'
        data_final = f'30-09-{year}'
    elif mes == 'Nov':
        data_inicial = f'01-10-{year}'
        data_final = f'31-10-{year}'
    elif mes == 'Dec':
        data_inicial = f'01-11-{year}'
        data_final = f'31-11-{year}'
    
    # Para a realização do desafio, obtendo os dados dos meses de janeiro e fevereiro juntos, 
    # eu travei a data inicial e final no link, porem para um ambiente de produção, onde o processo rodaria
    # todo mes, o que fica valendo é a data definida pelo if statement acima. 
    data_inicial = '01-01-2022'
    data_final = '28-02-2022'
    
    url = f'https://www.in.gov.br/consulta/-/buscar/dou?q=%22deferir+os+registros+e+as+peti%C3%A7%C3%B5es+dos+produtos+saneantes%22&s=todos&exactDate=personalizado&sortType=0&publishFrom={data_inicial}&publishTo={data_final}'
    print(url)

    browser = webdriver.Edge()
    browser.get(url)

    sleep(5)

    h5 = browser.find_elements_by_tag_name('h5')
    print(h5[0].text)
    n = len(h5)
    print(n)
    links_lista = []
    for i in range(n-1):
        a = h5[i].find_element_by_tag_name('a')
        links_lista.append(a.get_attribute('href'))
    
    browser.quit()
        
    return links_lista

def get_data(url):
    raw_texto = []
    link = url
    try:
        r = requests.get(link).text
    except:
        r = requests.get(link).text
    
    soup = BeautifulSoup(r, 'html.parser')
    for i in soup.find_all('p'):
        raw_texto.append(i.string)

    return raw_texto

def sep_empresas(lista):
    dados = lista[10:-7]
    dados = str(dados)
    dados = dados.split("'_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _'")
    lista2 = []
    for lista in dados:
        lista = str(lista)
        lista = lista.split(',')
        lista2.append(lista)
    # print(lista2)

    return lista2

def define_dataframe(lista):
    df_empresas = pd.DataFrame(columns=['EMPRESA', 'AUTORIZACAO', 'NOME_PRODUTO_MARCA', 'NUMERO_PROCESSO', 'NUMERO_REGISTRO', 'VENDA_EMPREGO',
                                     'VENCIMENTO', 'APRESENTACAO', 'VALIDADE_PRODUTO', 'CATEGORIA', 'ASSUNTO_PETICAO', 'EXPEDIENTE_PETICAO', 'VERSAO'])

    nome_empresa = 'NOME DA EMPRESA'
    autorizacao = 'AUTORIZAÇÃO'
    nome_produto = 'NOME DO PRODUTO E MARCA'
    numero_processo = 'NUMERO DE PROCESSO'
    numero_registro = 'NUMERO DE REGISTRO'
    venda_emprego = 'VENDA E EMPREGO'
    vencimento = 'VENCIMENTO'
    apresentacao = 'APRESENTAÇÃO'
    validade = 'VALIDADE DO PRODUTO'
    categoria = 'CATEGORIA'
    assunto = 'ASSUNTO DA PETIÇÃO'
    expediente = 'EXPEDIENTE DA PETIÇÃO'
    versao = 'VERSÃO'
    
    dados = lista

    for empresa in range(len(dados)):
        Empresa = [] #17
        Autorizacao = [] # 12
        Nome_produto = [] # 25
        Numero_processo = [] # 20
        Numero_registro = [] # 20
        Venda_emprego = [] # 17
        Vencimento = [] # 12
        Apresentacao = [] # 14
        Validade = [] # 21
        Categoria = [] # 11
        Assunto = [] # 20
        Expediente = [] # 23
        Versao = [] # 9

        for texto in dados[empresa]:
            if nome_empresa in texto:
                Empresa.append(texto[19:-1])
            elif autorizacao in texto:
                Autorizacao.append(texto[15:-1])
            elif nome_produto in texto:
                Nome_produto.append(texto[27:-1])
            elif numero_processo in texto:
                Numero_processo.append(texto[23:-1])
            elif numero_registro in texto:
                Numero_registro.append(texto[22:-1])
            elif venda_emprego in texto:
                Venda_emprego.append(texto[19:-1])
            elif vencimento in texto:
                Vencimento.append(texto[14:-1])
            elif apresentacao in texto:
                Apresentacao.append(texto[16:-1])
            elif validade in texto:
                Validade.append(texto[23:-1])
            elif categoria in texto:
                Categoria.append(texto[13:-1])
            elif assunto in texto:
                Assunto.append(texto[22:-1])
            elif expediente in texto:
                Expediente.append(texto[25:-1])
            elif versao in texto:
                Versao.append(texto[10:-1])

        Empresa = Empresa * len(Nome_produto)

        Autorizacao = Autorizacao * len(Nome_produto)

        if Versao and len(Versao) == len(Nome_produto):
            pass
        elif (len(Versao) != len(Nome_produto)) and len(Versao) > 0:
            mult = len(Nome_produto) - len(Versao)
            for i in range(mult):
                Versao.insert(0, 'None')
        else:
            Versao = ['None'] * len(Nome_produto)

        if Expediente and len(Expediente) == len(Nome_produto):
            pass
        elif (len(Expediente) != len(Nome_produto)) and len(Expediente) > 0:
            Expediente = [Expediente[0]]
            Expediente = Expediente * len(Nome_produto)
        else:
            Expediente = ['None'] * len(Nome_produto)

        if len(Validade) == len(Nome_produto):
            pass
        else:
            Validade = [Validade[0]]
            Validade = Validade * len(Nome_produto)

        if len(Vencimento) == len(Nome_produto):
            pass
        elif (len(Vencimento) != len(Nome_produto)) and len(Vencimento) > 0:
            Vencimento = [Vencimento[0]]
            Vencimento = Vencimento * len(Nome_produto)
        else:
            Vencimento = ['01/1970'] * len(Nome_produto)
            

        dict_df = {
            'EMPRESA': Empresa,
            'AUTORIZACAO': Autorizacao,
            'NOME_PRODUTO_MARCA': Nome_produto,
            'NUMERO_PROCESSO': Numero_processo,
            'NUMERO_REGISTRO': Numero_registro,
            'VENDA_EMPREGO': Venda_emprego,
            'VENCIMENTO': Vencimento,
            'APRESENTACAO': Apresentacao,
            'VALIDADE_PRODUTO': Validade,
            'CATEGORIA': Categoria,
            'ASSUNTO_PETICAO': Assunto,
            'EXPEDIENTE_PETICAO': Expediente,
            'VERSAO': Versao
        }

        df = pd.DataFrame(dict_df)
        df_empresas = df_empresas.append(df, ignore_index=True)

    return df_empresas

def add_resolucao(lista, df):
    resolucao = [lista[4]]
    len_resolucao = len(df)
    resolucao = resolucao * len_resolucao
    resolucao_dict = {"RESOLUCAO": resolucao}
    df_resolucao = pd.DataFrame(resolucao_dict)

    result = pd.concat([df_resolucao, df.reindex(df_resolucao.index)], axis=1)

    return result

def exporta_dataframe(df, nome_arquivo):
    today = datetime.now()
    date_for_csv = today.strftime('%d-%m-%Y_%H-%M-%S.%f')
    df.to_csv(f'{nome_arquivo}_{date_for_csv}.csv')

    
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def concatena_arquivos():
    today = datetime.now()
    date_for_file1 = today.strftime('%d-%m-%Y_%H-%M-%S-%f')
    date_for_file = today.strftime('%m')
    month = int(date_for_file)
    extracted_data = pd.DataFrame(columns=['RESOLUCAO', 'EMPRESA', 'AUTORIZACAO', 'NOME_PRODUTO_MARCA', 'NUMERO_PROCESSO', 'NUMERO_REGISTRO', 'VENDA_EMPREGO',
                                     'VENCIMENTO', 'APRESENTACAO', 'VALIDADE_PRODUTO', 'CATEGORIA', 'ASSUNTO_PETICAO', 'EXPEDIENTE_PETICAO', 'VERSAO'])
    #process all csv files
    for csvfile in glob.glob("resolucoes\*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        #df_empresas = df_empresas.append(df, ignore_index=True)

    extracted_data = extracted_data[['RESOLUCAO', 'EMPRESA', 'AUTORIZACAO', 'NOME_PRODUTO_MARCA', 'NUMERO_PROCESSO', 'NUMERO_REGISTRO', 'VENDA_EMPREGO',
                                     'VENCIMENTO', 'APRESENTACAO', 'VALIDADE_PRODUTO', 'CATEGORIA', 'ASSUNTO_PETICAO', 'EXPEDIENTE_PETICAO', 'VERSAO']]

    extracted_data['VENCIMENTO'] = pd.to_datetime(extracted_data['VENCIMENTO'], format="%m/%Y")
    
    extracted_data.to_excel(f"cliente\Resolucao_mes_{month - 1}_{date_for_file1}.xlsx", sheet_name="Sheet1")
        
    return extracted_data



def move_files(pasta_origem, pasta_destino):
    source = pasta_origem
    destination = pasta_destino
    files = os.listdir(source)

    for file in files:
        shutil.move(f"{source}/{file}", destination)
        
def envia_email(email_from, email_to, password):
    
    try:
        email = email_from
        cliente = email_to
        msg = MIMEMultipart()

        msg['From'] = email
        msg['To'] = cliente
        msg['Subject'] = "Testando enviar emails com axexo"
        
        body = "\nCorpo da mensagem"

        msg.attach(MIMEText(body, 'plain'))

        source = r'cliente'
        file = os.listdir(source)
        filename = file[0]
        print(file)

        attachment = open(f"{source}/{file[0]}", "rb")


        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        msg.attach(part)

        attachment.close()

        server = smtplib.SMTP('smtp.gmail.com: 587')
        server.starttls()
        server.login(email, password)
        text = msg.as_string()
        server.sendmail(email, cliente, text)
        server.quit()
        message = '\nEmail enviado com sucesso!'
    except:
        message = "\nErro ao enviar email"
        
    return message


def dataframe_categorias(df):
    df_categoria = df[['CATEGORIA']]
    df_categoria = df_categoria['CATEGORIA'].str.split(' ', expand=True, n=1)
    df_categoria.columns = ['categoria_id', 'descricao']
    df_categoria = df_categoria.drop_duplicates(subset=['categoria_id'])

    return df_categoria

def dataframe_assuntos(df):
    df_assunto = df[['ASSUNTO_PETICAO']]
    df_assunto = df_assunto['ASSUNTO_PETICAO'].str.split(' ', expand=True, n=1)
    df_assunto.columns = ['assunto_id', 'descricao']
    df_assunto = df_assunto.drop_duplicates(subset=['assunto_id'])

    return df_assunto

def dataframe_principal(df):
    df[['CATEGORIA_ID', 'CATEGORIA_NOME']] = df['CATEGORIA'].str.split(' ', expand=True, n=1)
    df[['ASSUNTO_ID', 'DESCRICAO_ASSUNTO']] = df['ASSUNTO_PETICAO'].str.split(' ', expand=True, n=1)
    df_final = df[['RESOLUCAO', 'EMPRESA', 'AUTORIZACAO', 'NOME_PRODUTO_MARCA', 'NUMERO_PROCESSO', 'NUMERO_REGISTRO', 'VENDA_EMPREGO', 
                           'VENCIMENTO', 'APRESENTACAO', 'VALIDADE_PRODUTO', 'CATEGORIA_ID', 'ASSUNTO_ID', 'EXPEDIENTE_PETICAO', 'VERSAO']]

    return df_final

def log(message):
    timestamp_format = '%d-%m-%Y %H:%M:%S:%f' # day-month-year hour:minute:second:microsecond
    now = datetime.now(tz=pytz.timezone('America/Sao_Paulo')) # get current timestamp in São Paulo timezone
    timestamp = now.strftime(timestamp_format)
    with open('logfile.txt', 'a') as f:
      f.write(f'{timestamp}, {message}\n')
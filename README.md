# TESTE PARA A POSIÇÃO DE ENGENHEIRO DE DADOS JR - MOVE

### **Linguagem utilizada**

- [Python](https://www.python.org/)

### **Bibliotecas utilizadas**

#### **Terceiros**

- [Selenium](https://www.selenium.dev/selenium/docs/api/py/index.html)

- [Requests](https://docs.python-requests.org/en/latest/)

- [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/)

- [Pandas](https://pandas.pydata.org/)

- [Openpyxl](https://openpyxl.readthedocs.io/en/stable/)

- [SqlAlchemy](https://www.sqlalchemy.org/)

- [Psycopg2](https://www.psycopg.org/docs/)

- [Diagrams](https://diagrams.mingrammer.com/)

#### **Built-in**

- [Datetime](https://docs.python.org/3/library/datetime.html)

- [Pytz](https://pypi.org/project/pytz/)

- [Glob](https://docs.python.org/3/library/glob.html)

- [Time](https://docs.python.org/3/library/time.html)

- [Os](https://docs.python.org/3/library/os.html?highlight=os#module-os)

- [Shutil](https://docs.python.org/3/library/shutil.html?highlight=shutil#module-shutil)

- [Smtplib](https://docs.python.org/3/library/smtplib.html?highlight=smtplib#module-smtplib)


### **Ferramentas utilizadas**

- [Vscode](https://code.visualstudio.com/)

- [Airflow](https://airflow.apache.org/)

- [Postgresql](https://www.postgresql.org/)

- [Git](https://git-scm.com/)

- [Miro](https://miro.com/app/dashboard/)

### **Arquitetura**

Processo Macro

![App Screenshot](/schemas/etl_proccess.png)

Etapas do processo no Airflow

![App Screenshot](/schemas/airflow_dag.png)

Etapas Detalhadas

![App Screenshot](/schemas/etl_explicado.png)

Para a realização deste processo eu criei seis arquivos python, tres para os testes no windows um chamado [funcoes.py](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/funcoes.py) onde se encontram todas as funçoes que serão usadas no processo, um [script.py](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/script.py) que eu usei para testar os processos, e um arquivo que sera utilizado como [base para a dag](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/script_dag.py), com as funçoes relativas as etapas do processo no airflow. E tres arquivos para o airflow, os arquivo [funçoes](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/airflow/minhas_funcoes.py) e o [arquivo base](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/airflow/script_to_dag.py) adaptados para o ambiente linux, passando o caminho completo das pastas onde os arquivos serão armazenados, e o arquivo da [DAG](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/airflow/move_dag.py).

Design do Banco de Dados 

![App Screenshot](/schemas/2022-03-26.png)

O script para a criação das tabelas esta [aqui](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/schemas/generate_tables.sql), e o arquivo.pgerd [aqui](https://github.com/lilacostaro/desafio_move_data_engineer_jr/blob/master/schemas/ERD.pgerd).

Além dessas tabelas, acredito que poderia se pensar em uma forma de tornar viavel tabelas dimensão referentes as 'empresas' e a 'venda e emprego', pois são campos que os valores tendem a se repetir. Pórem como esses campos não contam com valores de id, como o de categoria e assunto, esse processo acabaria sendo um pouco mais complicado. Creio que seria preciso gerar um dataframe com os valores unicos para cada uma dessas informacoes, carregar para o banco de dados que geraria um id automatico para elas, usando com primary key o campo do nome da empresa, e a descriçao de 'venda e emprego', pois esses são os valores que não podem se repetir. Realizao uma consulta ao banco de dados para obter a tabela atualizada, realizar o join apropriado pra atribuir os valores de id as tabela principal para por fim eliminar as colunas de descriçao e nome, e inserir os novos valores a tabela de saneantes.

## **Resultados** 

Os arquivos de backup do processo inicial se encontram na pasta [resolucoes_backup](https://github.com/lilacostaro/desafio_move_data_engineer_jr/tree/master/resolucoes_backup), o arquivo.xlsx final está na pasta [cliente_backup](https://github.com/lilacostaro/desafio_move_data_engineer_jr/tree/master/cliente_backup), e os arquivos backup das tabelas estão na pasta [database_backup](https://github.com/lilacostaro/desafio_move_data_engineer_jr/tree/master/database_backup).

O tempo total de execução do ultimo teste foi de 0:00:31.293162 segundos. E pode ser conferido no arquivo [logfile.txt]()

Abaixo seguem capturas de tela de consultas ao banco de dados. 

*Tabela Categoria*
![categoria](/schemas/categoria.png)

*Tabela assunto*
![assunto](/schemas/assuntos.png)

*Tabela saneantes*
![saneantes](/schemas/saneantes.png)

### **Observações**

Na etapa de coleta de dados, eu precisei tomar algumas decisões, de substituiçoes de dados, para que o processo podesse ocorrer, uma vez que os dados são bagunçados e os valores faltantes não seguem um padrão.  


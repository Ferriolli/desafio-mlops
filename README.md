
# desafio-mlops
Repositório contendo o código e as dependências necessárias para a execução do case.

Trata-se de um projeto que processa um arquivo CSV utilizando Spark, e opcionalmente salva as agregações realizadas no Redis.

Utiliza Airflow como orquestrador, em uma imagem Docker que possui Java como dependência, para facilitar a chamada ao Spark, caso contrário seria necessário usar o DockerOperator do Airflow, porém isso traria uma complexidade muito mais elevada, e dependeria de configurações adicionais por parte do usuário.
Possui uma agregação simples feita em PySpark, e pode ou não salvar os dados no Redis, dar a opção ao usuário aumenta a flexibilidade e permite realizar testes sem que os dados sejam salvos ou sobrescritos/alterados.

Além disso, o repositório está configurado para executar Linting (Black) e testes unitários (pytest e pytest-cov) sempre que recebe um novo push em qualquer branch.
### Configuração inicial

Clone o repositório

```
# Para clonar com HTTPS
git clone https://github.com/Ferriolli/desafio-mlops.git

# Ou com SSH
git clone git@github.com:Ferriolli/desafio-mlops.git
```

Para a primeira execução do projeto, são necessários alguns passos para configurar o Airflow.

Crie um arquivo .env na raíz do projeto, e adicione as seguintes variáveis de ambiente.

| Variável                        | Valor de exemplo                                       | Explicação                                                          |
| ------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------- |
| AIRFLOW__CORE__EXECUTOR         | LocalExecutor                                          | Define o tipo de executor que o Airflow vai usar.                   |
| AIRFLOW__CORE__SQL_ALCHEMY_CONN | postgresql+psycopg2://airflow:airflow@postgres/airflow | Define a string de conexão com o banco.                             |
| AIRFLOW__WEBSERVER__SECRET_KEY  | SUPER_SECRET_KEY                                       | Serve para assinar cookies da interface web do airflow.             |
| AIRFLOW__CORE__FERNET_KEY       | BDV4hA56l2t8BdkDXDWJooAWQdKj4xwBHWJLoAKNQuw=           | Serve para criptografar informações sensíveis armazenadas no banco. |
| AIRFLOW__CORE__LOAD_EXAMPLES    | False                                                  | Serve para carregar (ou não) DAGs de exemplo.                       |
| REDIS_HOST                      | redis                                                  | Host do redis usado como feature store                              |
| REDIS_PORT                      | 6379                                                   | Porta do redis usado como feature store                             |
| REDIS_DB                        | 0                                                      | DB do redis usado como feature store                                |

> [!WARNING]
Uma nova fernet key deve ser gerada para cada nova instância do Airflow. Para testes, é possível usar a que está na coluna "Valor de exemplo", mas caso queira gerar uma nova, siga os passos abaixo
Para gerar uma nova fernet key, execute o seguinte comando em um terminal com Python instalado (É necessário ter a biblioteca cryptography instalada).
```
# Para instalar a biblioteca (caso ainda não tenha)
pip install cryptography

# Para gerar a chave fernet, execute o seguinte comando e cole a saída como valor
# da variável de ambiente: AIRFLOW__CORE__FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Após a criação do arquivo conforme descrito acima, vá até a pasta docker/ dentro do projeto, e inicie os serviços com o Docker.

```
docker compose up -d
```

Com isso, todos as imagens serão baixadas/montadas e os serviços serão iniciados.

Após o fim da execução do comando anterior, aguarde até que os serviços iniciem, e abra o Airflow.
Você pode checar o status dos serviços com o comando abaixo, que mostra várias informações dos containers que estão sendo executados.
```
docker ps
```

No Browser, acesse a UI do AIrflow em: localhost:8080.
Será necessário usuário e senha para login, utilize admin como usuário, e admin como senha.

Antes de executar a DAG **feature_engineering**, é necessário configurar uma conexão no Airflow, para isso, faça o seguinte.
1. Na aba admin, clique em Connections
2. Clique no botão de "+"
3. Em Connection Id, preencha com spark_custom
4. Em Connection Type, selecione **Spark Connect**
5. Em Host, preencha com: **spark://spark-master**
6. Port, preencha com: **7077**


### Execução

A partir daqui, a configuração inicial já foi feita, agora partiremos para a execução do código.
Conforme a configuração dos containers, a DAG já aparecerá na tela inicial do Airflow, portanto, clique no nome da DAG, **feature_enginnering**.

Essa DAG é composta de 4 steps, são eles:
1. iniciar_processamento_task: Um BashOperator que apenas printa um log (Iniciando pipeline...)
2. check_redis: Um PythonOperator que faz um health check no Redis, basicamente faz um ping na conexão, e levanta um erro caso não obtenha um retorno do serviço.
3. processar_e_salvar_features: Um SparkSubmitOperator, o serviço principal da DAG, envia um comando para o Spark com os parametros necessários e inicia o processamento.
4. end_task, um DummyOperator que apenas serve como visual.

Será mostrada a tela de execução da DAG, onde são apresentados os LOGS das execuções.
No canto superior direito, clique no botão de "Play", a seguinte tela será mostrada:

![Trigger da dag no Airflow](assets/airflow_dag_trigger.png)

Os parâmetros padrão são os que estão mostrados no print acima. Já está configurado para executar de forma correta, mas, está parametrizado.
O volume está configurado para montar o arquivo novas_empresas.csv no caminho **/mlops/data/novas_empresas.csv** dentro do Container.

O parâmetro save_to_db indica para a DAG se os dados devem ser salvos no REDIS, se save_to_db for **TRUE**, os dados serão salvos no REDIS, caso contrário, ao fim da execução da Pipeline, será printado no LOG do Airflow um pequeno Dataframe com as agregações feitas pelo Spark.

Após configurar da forma desejada, clique em **Trigger**.

Você será levado de volta para a tela de execução da DAG, onde as tasks serão executadas e seus respectivos status serão mostrados na parte esquerda da tela.
Ao fim da execução, em caso de sucesso, os dados podem ser visualizados pelo Redis-Commander, uma UI WEB que acessa o REDIS. É possível acessá-la em: http://localhost:8082/
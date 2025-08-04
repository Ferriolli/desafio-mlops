 
## desafio-mlops

Repositório contendo o código e as dependências necessárias para a execução do case.
## Tecnologias usadas

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

### Breve descrição do projeto

Este projeto processa um arquivo CSV utilizando Spark, e opcionalmente salva as agregações realizadas no Redis.

Utiliza Airflow como orquestrador, em uma imagem Docker que já contém Java, facilitando a chamada ao Spark.
Essa abordagem evita a necessidade de usar o DockerOperator do Airflow, o que aumentaria a complexidade, e exigiria configurações adicionais.

A agregação feita em PySpark é simples, e o sistema permite ao usuário optar por salvar os dados no Redis ou não, aumentando a flexibilidade para testes sem alterar ou sobrescrever os dados existentes.
A visualização dos dados no Redis pode ser feita através do redis-commander (uma UI web para Redis).

Além disso, o repositório está configurado para executar Linting (Black) e testes unitários (pytest e pytest-cov) automaticamente a cada push em qualquer branch.
### Configuração inicial

Clone o repositório

```
# Clonar via HTTPS
git clone https://github.com/Ferriolli/desafio-mlops.git

# Ou via SSH
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
Uma nova fernet key deve ser gerada para cada nova instância do Airflow.
Para testes, é possível usar a que está na coluna "Valor de exemplo", mas caso queira gerar uma nova, siga os passos abaixo.
Para gerar uma nova fernet key, execute o seguinte comando em um terminal com Python instalado (É necessário ter a biblioteca cryptography instalada).
Caso troque a FERNET key após a primeira execução do AIRFLOW, todas as connections serão invalidadas, será necessário recriar a conexão com o Spark (passo-a-passo mais abaixo).
```
# Para instalar a biblioteca (caso ainda não tenha)
pip install cryptography

# Para gerar a chave fernet, execute o seguinte comando e cole a saída para a variável: AIRFLOW__CORE__FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Após a criação do arquivo conforme descrito acima, vá até a pasta docker/ dentro do projeto, e inicie os serviços com o Docker.

```
docker compose up -d
```

Com isso, todas as imagens serão baixadas/montadas e os serviços serão iniciados.

Após o fim da execução do comando anterior, aguarde até que os serviços iniciem, e abra o Airflow.
Você pode checar o status dos serviços com o comando abaixo, que mostra várias informações dos containers que estão sendo executados.
```
docker ps
```

No Browser, acesse a UI do Airflow em: localhost:8080.
Usuário e senha padrão: `admin` / `admin`

O docker-compose já configura tudo necessário para executar a DAG (adiciona o usuário, e cria a conexão com o Spark).
### Visualização da DAG

A partir daqui, a configuração inicial já foi feita, agora partiremos para a execução do código.
Conforme a configuração dos containers, a DAG já aparecerá na tela inicial do Airflow, portanto, clique no nome da DAG, **feature_engineering**.

Essa DAG é composta de 4 steps, são eles:
1. iniciar_processamento_task: Um BashOperator que apenas printa um log (Iniciando pipeline...)
2. check_redis: um PythonOperator que faz um health check no Redis, basicamente faz um ping na conexão, e levanta um erro caso não obtenha um retorno do serviço.
3. processar_e_salvar_features: um SparkSubmitOperator, o serviço principal da DAG, envia um comando para o Spark com os parâmetros necessários e inicia o processamento.
4. end_task, um DummyOperator que apenas serve como visual.

Será mostrada a tela de execução da DAG, onde são apresentados os LOGS das execuções.

### Execução da DAG

> [!NOTE]
> Já tem um comando no compose que torna a DAG ativa na primeira vez que o container sobe, ou seja, ao entrar na UI, já é para uma execução ter sido completada.
> Os passos abaixos são para executar a DAG manualmente.

No canto superior direito, clique no botão de "Play", a seguinte tela será mostrada:

![Trigger da dag no Airflow](assets/airflow_dag_trigger.png)

Os parâmetros padrão são os que estão mostrados no print acima. Já está configurado para executar de forma correta, mas, está parametrizado.
Existe um volume configurado para montar o arquivo novas_empresas.csv no caminho **/mlops/data/novas_empresas.csv** dentro do Container.

O parâmetro `save_to_db` indica para a DAG se os dados devem ser salvos no Redis:
- (`TRUE`): Envia os dados para o Redis ao fim da execução.
- (`False`): Apenas printa um pequeno Dataframe com as agregações ao final da execução.

Após configurar da forma desejada, clique em **Trigger**.

Você será levado de volta para a tela de execução da DAG, onde as tasks serão executadas e seus respectivos status serão mostrados na parte esquerda da tela.

## Visualização dos dados

Ao fim da execução, em caso de sucesso, os dados podem ser visualizados pelo Redis-Commander, uma UI WEB que acessa o REDIS. É possível acessá-la em: http://localhost:8082/

## Execução de Lint ou testes

As dependências necessárias para executar os testes e o lint estão listadas no arquivo requirements.txt na **raíz do projeto**.

Para executar lint no código é necessário o módulo `Black`, utilizado da seguinte forma:

```
# Na raíz do projeto
# Caso deseje que o black formate os arquivos, execute:
black .

# Case deseje que o black apenas informe quais arquivos podem ser formatados, execute:
black --check .
```

Para executar os testes unitários sem verificação de cobertura, é necessário o módulo `pytest`, caso deseje verificação de cobertura, também é necessário o `pytest-cov`
É necessário ter Java instalado na máquina caso queira executar os testes localmente, por conta do Spark.

```
# Na raíz do projeto
# Para testes unitários sem cobertura, execute:
pytest -s

# Para testes unitários com cobertura, execute:
pytest -s --cov jobs/

# Ou, para testes unitários com cobertura e geração de relatório, execute:
pytest -s --cov jobs/ --cov-report=html:tests/test_results/
```

Caso não possua Java instalado, basta executar o seguinte comando, que sobe um container já com as dependências e executa os testes unitários.

```
# Na raíz do projeto
docker compose -f tests/docker/docker-compose.tests.yml up -d
```

A execução do comando acima salva os relatórios dos testes em `tests/test_results`.

### Criação da conexão com o Spark
Caso a FERNET key seja alterada após a inicialização do Airflow, ele perde a capacidade de decodificar informações de conexões, nesse caso, é necessário recriar a conexão com o Spark.

1. Na aba Admin (topo da tela) clique em Connections
2. Clique no "+" para adicionar uma conexão
3. Preencha as informações da seguinte forma:
    - Connection id: **spark_custom**
    - Connection Type: **Spark Connect**
    - Host: **spark://spark-master**
    - Port: **7077**
4. Clique em "Save"


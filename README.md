# PROJETO METEOL 



## Overview
O projeto Meteol visa extrair e processar dados meteorológicos, proporcionando análises fundamentais para diversas aplicações. Utilizando técnicas avançadas de engenharia de dados, busca-se fornecer insights valiosos e apoiar decisões informadas em setores como agricultura, energia e previsão climática.
Este projeto também cria um ambiente configurado para a realização destes processos.


##### Clone o repositório em sua máquina local Linux e acesse a pasta Meteol_Project
```sh
git clone https://github.com/Syds674/Meteol_Project.git
```
```sh
cd Meteol_Project
```

##### Entre no diretório meteol e conceda permisões necessárias para o arquivo de criação de pastas
```sh
cd meteol
```
```sh
sed -i -e 's/\r$//' cria_dir.sh
```

##### Volte para a pasta Meteol_Project e execute o seguinte ocmando para a construção do ambiente:
```sh
sudo docker-compose -f all-docker-compose.yaml up
```

##### Em seguida, para realizar a cópia da estrutura de scripts para dentro do namenode, abra um novo terminal no diretório Meteol_Project e executar: 
```sh
chmod +x copy_meteol_structure.sh
```
```sh
./copy_indicium_structure.sh
```
Obs.: Após a ultima execução irá conectar automáticamente no namenode.


##### Estando conectado no namenode, executar:
```sh
chmod 775 /meteol/cria_dir.sh
```
```sh
./meteol/cria_dir.sh
```
Obs.: Este comando irá criar a estrutura de pastas do HDFS

##### Executar o comando de criação de database no Hive:
```sh
spark-submit /meteol/create_database.py
```


## Extração e exportação dos dados

### Para a extração dos dados temos a pasta /meteol/scripts contendo os scripts que executarão todo processo de ETL
```
extracao_{data_de_extração_de_dados}.py
```
- Script que extrai as tabelas do site https://portal.inmet.gov.br/uploads/dadoshistoricos/ disponibilizando os arquivos dentro do diretório do hdfs no formato /hdfs/data/order/{nome_tabela}/input/{nome_tabela}.csv 


```
process_{nome_tabela}.py 
```
- Script que une os asquivos csv em um data frame chamado table_{data_dos_dados_extraídos} e salva a tabela no hive.



##### Exemplo de execução do scripts
```sh
spark-submit spark-submit /meteol/scripts/extract_{data_dos_dados_extraídos}.py
```
```sh
spark-submit /indicium/scripts/process_{data_dos_dados_extraídos}.py 
```



## Para consulta ao Hive e acesso as tabelas, utilize a seguinte URL

|Applicação | Url |
|--- |--- |
| Hue | http://localhost:8888 |

User login: hue
Password: hue


## Dependências
- [Docker](https://docs.docker.com/):
    1. Install Docker on ubuntu: https://docs.docker.com/engine/install/ubuntu/
    2. Install Docker on windows: https://docs.docker.com/desktop/install/windows-install/
 
## Para a realização deste desafio foi utilizado o repositório:
https://github.com/mrugankray/Big-Data-Cluster

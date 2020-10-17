# Estudo GCP

Este estudo foi baseado na documentação:
- https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python




## Configuração de Ambiente Virtual

1 - Instalando o ambiente virtual Python

`pip install virtualenv`

2 - Criando o ambiente virtual

`python -m venv env`

2 - Habilitando o ambiente virtual

`.\env\Scripts\activate`

## Instalando os pacotes a partir do requirement file

`pip install -r requirements.txt`


## Primeiro teste GCP

Este comando tem como objetivo executar o programa wordcount.py sobe uma obra de Shakespeare e gerar a saída outputs.

`python -m apache_beam.examples.wordcount --output outputs` 

Este exemplo rodará o mesmo programa, porém no Dataflow.

`
python -m apache_beam.examples.wordcount ^
      --region us-east1 ^
      --input gs://dataflow-samples/shakespeare/kinglear.txt ^
      --output gs://fk-teste/wordcount/outputs ^
      --runner DataflowRunner ^
      --project teste-dotz-292803 ^
      --temp_location gs://fk-teste/tmp/
`

## Visualizar os resultados usando o GCP

`gsutil ls -lh "gs://fk-teste/wordcount/outputs*"`

Pode ser que receba uma mensagem de '401', nesse caso, rode o comando `gsutil config` para setar as credenciais.

Para ver os resultados desses arquivos, execute o comando a seguir:

`gsutil cat "gs://$BUCKET/wordcount/outputs*"`




## Rodando Docker
Referencia: https://cloud.google.com/sdk/docs/downloads-docker

`docker pull gcr.io/google.com/cloudsdktool/cloud-sdk:latest`
`docker run --rm gcr.io/google.com/cloudsdktool/cloud-sdk:latest gcloud version`
`docker run -ti --name gcloud-config gcr.io/google.com/cloudsdktool/cloud-sdk gcloud auth login`

Este comando entra no prompt
`docker run -ti --rm -v "$PWD":/my_app -w "/my_app" gcr.io/google.com/cloudsdktool/cloud-sdk bin/bash`

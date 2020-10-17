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
python -m apache_beam.examples.wordcount \
      --region $REGION \
      --input gs://dataflow-samples/shakespeare/kinglear.txt \
      --output gs://$BUCKET/wordcount/outputs \
      --runner DataflowRunner \
      --project $PROJECT \
      --temp_location gs://$BUCKET/tmp/
`
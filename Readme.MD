# Teste de Engenheiro de dados para o Grupo Dotz

Este material é parte do processo de seleção de Engenheiros de Dados do Grupo Dotz.

Para este desafio foi disponilizado um conjunto de dados de uma empresa que produz e vende máquinas industriais. O objetivo deste desafio é (1) Fazer a modelagem conceitual dos dados, (2) Criar a infraestrutura necessária e (3) criar todos os artefatos necessários para carregar os arquivos para o banco criado.

Para a realização deste teste foi sugerido a utilização dos recursos da nuvem do Google (GCP) e o Data Flow (GCP). 

## Desafios
- Estudo e familização com os serviços da GCP
- Criação de script para envio do Dataset para o Bucket da Google
- Criação de uma pipeline usando Apache Beam
- Utilização do DataFlow

# Requisito 1 - Modelagem conceitual dos Dados

Após um estudo da base, foi identificado a necessidade de criar basicamente 4 entidades e estabelecer uma cardinalidade N:M entre **Tube_Assembly** e **component**, para correção fugir da cardinalidade N:M foi inserido uma entidade chamada **product_structure**, assim:
- 1 conjunto pode ter 1 ou mais componentes;
- 1 componente pode estar presente em 1 ou mais conjuntos.

especial, 
![alt text](images/001.png)


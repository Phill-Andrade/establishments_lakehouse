# Template Pipeline

Template code-first para engenheiros de dados criarem pipelines PySpark independentes, executados no YARN da plataforma e orquestrados pelo Airflow.

> Status: estrutura code-first em construção. O ciclo de vida da `SparkSession` está implementado; os demais módulos serão adicionados com comportamento testável.

---

## Índice

- [Início rápido](#início-rápido)
- [Uso](#uso)
- [Arquitetura](#arquitetura)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Responsabilidades dos módulos](#responsabilidades-dos-módulos)
- [Diretrizes de contribuição](#diretrizes-de-contribuição)
- [Versionamento e releases](#versionamento-e-releases)
- [Personas e responsabilidades](#personas-e-responsabilidades)

---

## Início rápido

### Pré-requisitos

- Python compatível com a versão declarada no [`pyproject.toml`](pyproject.toml).
- Poetry para instalar e validar as dependências do projeto.
- Docker para construir e executar a imagem do pipeline.
- Imagem local `hadoop_platform` fornecida pela plataforma de dados.
- Rede Docker `data_platform_network` criada pela plataforma.
- Serviços HDFS, YARN e Hive Metastore disponíveis.

As versões e dependências Python são mantidas em [`pyproject.toml`](pyproject.toml) e [`poetry.lock`](poetry.lock).

### Instalação

Com o ambiente virtual ativado:

```bash
poetry install
```

### Execução mínima

Ainda não existe uma aplicação executável. O comando será documentado após a implementação do `main.py` e da submissão direta com `spark-submit`.

### Testes

Os testes unitários usam `pytest` e mocks para não iniciar uma sessão Spark real:

```bash
poetry run pytest -s
```

A suíte atual valida criação, normalização do nome, encerramento e falhas do ciclo de vida da `SparkSession`.

---

## Uso

### Casos de uso suportados

- **Criar um pipeline independente a partir do template**
  - Entrada: cópia deste repositório e implementação das responsabilidades específicas do pipeline.
  - Saída: repositório versionado e imagem de container próprios para o novo pipeline.

- **Executar processamento PySpark na plataforma de dados**
  - Entrada: código code-first do pipeline e parâmetros fornecidos pela orquestração.
  - Saída: dados processados pelo Spark e persistidos pelo módulo de escrita definido pelo pipeline.

- **Orquestrar o pipeline com Airflow**
  - Entrada: imagem versionada do pipeline e parâmetros dinâmicos da execução.
  - Saída: submissão acompanhada pelo Airflow e aplicação executada no YARN.

Cada repositório criado a partir deste template representa um pipeline implantável de forma independente. A coordenação entre pipelines pertence ao Airflow, não ao código de outro pipeline.

---

## Arquitetura

O template ocupa a camada de execução entre o Airflow e a plataforma de dados:

```text
Airflow
  → inicia o container do pipeline
  → o container executa spark-submit
  → YARN aloca driver e executores
  → a aplicação PySpark executa o fluxo code-first
  → o módulo de escrita persiste o resultado
```

As configurações estáveis de Spark, YARN, HDFS e Hive são herdadas da imagem `hadoop_platform`. O contrato esperado está documentado em [`configs/platform.yaml`](configs/platform.yaml).

O fluxo interno previsto é:

```text
main
  → carrega settings
  → cria SparkSession
  → extrai os dados
  → valida a entrada
  → transforma os dados
  → valida o resultado
  → escreve a saída
  → encerra a SparkSession
```

### Suporte às camadas Raw, Bronze, Silver e Gold

As camadas representam o estágio dos dados no lakehouse, não módulos ou diretórios do código. Cada pipeline define sua camada de entrada e sua camada de saída, reutilizando as mesmas responsabilidades de extração, transformação, Data Quality e escrita.

| Fluxo | Responsabilidade do pipeline |
| --- | --- |
| Fonte → Raw | Extrair os dados da origem e persistir uma representação fiel para rastreabilidade. |
| Raw → Bronze | Aplicar padronizações técnicas e verificações estruturais antes da escrita. |
| Bronze → Silver | Limpar, deduplicar e conformar os dados para uso compartilhado. |
| Silver → Gold | Aplicar regras de negócio e produzir dados orientados ao consumo. |

Uma nova tabela utiliza a mesma estrutura de módulos e define somente suas fontes, regras e destinos específicos. Não serão criados arquivos como `raw.py`, `bronze.py`, `silver.py` ou `gold.py`, porque isso acoplaria a organização do código à camada de armazenamento e duplicaria responsabilidades já existentes.

### Estratégia de configuração

O template segue code-first:

- regras de transformação e Data Quality ficam em código Python;
- configurações estáveis do cluster permanecem no `spark-defaults.conf` da plataforma;
- parâmetros dinâmicos da execução serão recebidos por argumentos de linha de comando;
- valores específicos do ambiente poderão ser fornecidos por variáveis de ambiente;
- segredos não devem ser gravados no código ou no repositório;
- arquivos YAML de job não fazem parte do runtime do pipeline.

---

## Estrutura do projeto

A árvore abaixo representa a arquitetura-alvo. Módulos planejados não serão criados como arquivos vazios; cada um será adicionado na branch que implementar sua responsabilidade.

```text
.
├── configs/
│   └── platform.yaml
├── docker/
│   └── Dockerfile
├── src/
│   └── template_pipeline/
│       ├── __init__.py
│       ├── main.py
│       ├── settings.py
│       ├── spark.py
│       ├── extraction.py
│       ├── transformation.py
│       ├── quality.py
│       └── writing.py
├── tests/
│   ├── unit/
│   └── integration/
├── .gitignore
├── poetry.lock
├── pyproject.toml
└── README.md
```

O Git não versiona diretórios vazios. Por isso, a árvore documenta o padrão completo, enquanto o repositório físico cresce incrementalmente com arquivos funcionais.

---

## Responsabilidades dos módulos

### `main.py`

Ponto de entrada e composition root do pipeline. Coordena o ciclo de execução e delega leitura, validação, transformação e escrita sem implementar essas regras.

### `settings.py`

Centraliza parâmetros de execução e valores dependentes do ambiente. Não contém segredos, regras de negócio ou configurações estáveis já fornecidas pela plataforma.

### `spark.py`

Cria e encerra a `SparkSession`. Não duplica `spark.master`, `spark.submit.deployMode`, configurações XML ou defaults herdados da imagem da plataforma.

### `extraction.py`

Lê as fontes necessárias e devolve `DataFrame`. Não transforma nem grava dados.

### `transformation.py`

Aplica regras de negócio sobre `DataFrame` e devolve o resultado transformado. Deve evitar operações de entrada e saída para permanecer testável.

### `quality.py`

Executa verificações de Data Quality e interrompe o fluxo quando uma condição obrigatória não é atendida. Pode ser chamada após a extração e após a transformação.

### `writing.py`

Persiste o resultado no destino definido pelo pipeline. Centraliza opções de escrita, incluindo Hudi quando aplicável, sem incorporar regras de transformação.

### `tests/unit/`

Contém testes isolados para regras de transformação, configuração e Data Quality que não dependam dos serviços da plataforma.

### `tests/integration/`

Contém testes que dependam de Spark ou das integrações de leitura e escrita. Será criado quando existir o primeiro teste dessa categoria.

Não serão criados módulos genéricos como `utils.py`, `helpers.py`, fábricas ou interfaces sem uma responsabilidade concreta.

---

## Diretrizes de contribuição

### Fluxo de Pull Request

1. Atualize a branch `dev` com `git pull --ff-only`.
2. Crie uma branch no padrão `feat/<change_name>`, `fix/<change_name>` ou `hotfix/<change_name>`.
3. Implemente uma responsabilidade coesa e mantenha o Pull Request pequeno.
4. Execute as validações disponíveis para o escopo alterado.
5. Revise o diff antes do commit manual de aceite.
6. Abra o Pull Request para `dev`, exceto no fluxo explícito de hotfix.

### Padrões de qualidade

- Código Python simples, explícito e legível.
- SOLID e Object Calisthenics aplicados somente quando melhorarem o desenho.
- Fail First para entradas e condições obrigatórias.
- Tell, Don't Ask na coordenação entre responsabilidades.
- Bibliotecas maduras preferidas quando reduzirem a complexidade total.
- Abstrações adicionadas somente após uma necessidade concreta.
- Testes: `pytest`.
- Linter: ainda não configurado.
- Política de cobertura: ainda não definida.

---

## Versionamento e releases

- A versão atual está declarada em [`pyproject.toml`](pyproject.toml).
- A estratégia de versionamento ainda não foi definida.
- O mecanismo de release ainda não foi definido.
- Não existe `CHANGELOG.md` nesta fase de fundação.
- A política para breaking changes ainda não foi definida.

---

## Personas e responsabilidades

### Plataforma de dados

Mantém Spark, YARN, HDFS, Hudi, Hive Metastore, PostgreSQL, Spark History Server, configurações XML, `spark-defaults.conf`, imagem-base e rede Docker.

### Airflow

Mantém agendamento, dependências entre pipelines, retries, timeout e parâmetros dinâmicos da execução.

### Pipeline

Mantém o código de extração, transformação, Data Quality, escrita e submissão da aplicação ao YARN. Não recria serviços pertencentes à plataforma ou ao Airflow.

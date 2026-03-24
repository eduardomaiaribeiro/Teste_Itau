# Teste Técnico - Engenheiro de Dados PySpark

## Objetivo
Analisar dados de clientes e pedidos em JSON, atendendo aos requisitos:
1. Data Quality
2. Agregação por cliente
3. Estatísticas
4. Clientes acima da média
5. Clientes entre percentis 10 e 90

## Anexei aqui um arquivo chamado codigo_inicial.md, que foi o meu primeiro código, tudo em uma única função main, sem segregação de responsabilidades
## A idéia é mostrar como eu evolui desde o primeiro código até o código final já com as melhorias implementadas.

## Estrutura
- `app/`: código principal
- `tests/`: testes unitários
- `.env.example`: configuração de ambiente
- `requirements.txt`: dependências

## Como executar

### 1. Criar ambiente virtual
```bash
python -m venv .venv
```

### 2. Ativar ambiente virtual
```bash
.venv\Scripts\activate
```

### 3. Instalar dependências
```bash
pip install -r requirements.txt
```

### 4. Executar o pipeline
```bash
python -m app.main
```

### 5. Executar testes
```bash
pytest
```

## Configuração
Crie um arquivo `.env` na raiz do projeto:
```env
CLIENTS_PATH=data/clients.json
ORDERS_PATH=data/orders.json
SHOW_ROWS=20
```
>*Dica: Defina `SHOW_ROWS=0` caso queira exibir todos os registros no terminal em vez de limitar.*

## Requisitos
- Python 3.8+
- PySpark 3.x
- pytest
- Java 8 ou 11+ (com `JAVA_HOME` configurado)
- Windows: Binários do Hadoop (`winutils.exe`) configurados via `HADOOP_HOME` para permitir execuções locais do Spark

## Licença
MIT
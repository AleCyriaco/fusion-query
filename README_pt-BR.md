<p align="center">
  <img src="docs/logo.svg" width="120" alt="fusion-query logo">
</p>

<h1 align="center">fusion-query</h1>

<p align="center">
  <strong>Motor universal de consultas SQL para Oracle Fusion Cloud via BI Publisher</strong>
</p>

<p align="center">
  <a href="README.md">English</a> |
  <a href="README_es.md">Espanol</a>
</p>

---

Execute consultas SQL arbitrarias no Oracle Fusion Cloud (ERP, HCM, SCM) usando **apenas URL, usuario e senha**. Sem JDBC, sem VPN, sem acesso direto ao banco de dados.

## Como funciona

```
SQL → gzip → base64 → HTTP POST para BI Publisher → PL/SQL REF CURSOR → CSV → linhas parseadas
```

O fusion-query usa o Oracle BI Publisher com um relatorio proxy leve. O relatorio contem um bloco PL/SQL que recebe o SQL comprimido, executa via REF CURSOR no banco Fusion e retorna os resultados em CSV delimitado por pipe. Funciona com as APIs REST e SOAP, selecionando automaticamente o melhor transporte para sua instancia.

```sql
-- PL/SQL dentro do Data Model do relatorio proxy:
DECLARE
  TYPE CurType IS REF CURSOR;
  xdo_cursor CurType;
BEGIN
  OPEN :xdo_cursor FOR
    utl_raw.cast_to_varchar2(
      UTL_COMPRESS.lz_uncompress(
        TO_BLOB(utl_encode.base64_decode(
          UTL_RAW.CAST_TO_RAW(:P_B64_CONTENT)
        ))
      )
    );
END;
```

---

## Instalacao

```bash
pip install fusion-query
```

Com extras:
```bash
pip install fusion-query[server]   # Servidor REST API (FastAPI + Uvicorn)
pip install fusion-query[cli]      # Formatacao de tabela (Rich)
pip install fusion-query[all]      # Tudo
```

---

## Inicio rapido

### Apenas conecte e consulte — sem configuracao previa

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "usuario", "senha")

result = client.query("SELECT USER_NAME, EMAIL_ADDRESS FROM PER_USERS")
for row in result.rows:
    print(row["USER_NAME"], row["EMAIL_ADDRESS"])
```

O relatorio proxy e **implantado automaticamente** na sua pasta pessoal do BIP (`/~usuario/FusionQuery/`) no primeiro uso. Nao requer papel de Administrador BI — qualquer usuario autenticado pode comecar a consultar imediatamente.

---

## Tres formas de usar

### Biblioteca Python

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")

# Pagina unica (ate 1000 linhas)
result = client.query("SELECT * FROM PER_USERS")

# Auto-paginacao (todas as linhas)
result = client.query_all("SELECT * FROM PER_USERS ORDER BY USER_ID", max_rows=5000)

# Paginacao manual
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM")
while page.has_next:
    page = client.fetch_next(page)
```

### CLI

```bash
# Executar consulta
fusion-query query --url https://... --user admin "SELECT SYSDATE FROM DUAL"

# Saida JSON
fusion-query query --url ... --user admin -f json "SELECT * FROM PER_USERS"

# Buscar todas as linhas (auto-paginacao)
fusion-query query --url ... --user admin --all --max-rows 5000 \
  "SELECT * FROM PER_USERS ORDER BY USER_ID"

# Testar conexao
fusion-query test --url https://... --user admin
```

### API REST (qualquer linguagem: Java, Rust, JS, Go, etc.)

```bash
fusion-query serve --port 8000
# Documentacao da API em http://localhost:8000/docs
```

```bash
# Conectar
curl -X POST http://localhost:8000/connect \
  -H "Content-Type: application/json" \
  -d '{"name":"prod", "url":"https://xxxx.oraclecloud.com",
       "username":"admin", "password":"secret"}'

# Consultar
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT USER_NAME FROM PER_USERS", "connection":"prod"}'

# Auto-paginacao
curl -X POST http://localhost:8000/query/all \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM PER_USERS ORDER BY USER_ID",
       "connection":"prod", "max_rows":5000}'
```

---

## Paginacao

### O problema

O Oracle BI Publisher impoe um **limite de ~1000 linhas** por execucao de relatorio. Consultas que retornam mais sao silenciosamente truncadas.

### A solucao

O fusion-query envolve seu SQL com `OFFSET ... FETCH NEXT ... ROWS ONLY` e faz multiplas requisicoes HTTP para buscar todos os dados de forma transparente em paginas.

### PageInfo — contrato universal de paginacao

Toda resposta inclui um objeto `page_info`. **Todos os drivers (Python, Java, Rust, JS) devem implementar esta mesma estrutura:**

```json
{
  "page_info": {
    "page": 0,
    "page_size": 1000,
    "offset": 0,
    "rows_returned": 1000,
    "has_next": true,
    "total_fetched": 1000,
    "max_rows": null,
    "exhausted": false
  }
}
```

| Campo | Tipo | Descricao |
|-------|------|-----------|
| `page` | int | Numero da pagina atual (base 0) |
| `page_size` | int | Linhas solicitadas por pagina (max 1000) |
| `offset` | int | OFFSET SQL usado nesta pagina |
| `rows_returned` | int | Linhas reais nesta pagina |
| `has_next` | bool | `true` se `rows_returned == page_size` (provavelmente ha mais dados) |
| `total_fetched` | int | Total acumulado de linhas em todas as paginas |
| `max_rows` | int/null | Limite superior definido pelo chamador (null = ilimitado) |
| `exhausted` | bool | `true` se nao ha mais dados ou max_rows foi atingido |

### Algoritmo de paginacao (para implementadores de drivers)

```
funcao query_all(sql, page_size=1000, max_rows=None):
    todas_linhas = []
    pagina = 0

    loop:
        sql_paginado = envolver_com_offset(sql, pagina * page_size, page_size)
        codificado = base64(gzip(sql_paginado))
        resposta = HTTP_POST(url_bip, {P_B64_CONTENT: codificado})
        linhas = parse_csv(base64_decode(resposta.reportBytes))

        todas_linhas.adicionar(linhas)

        se len(linhas) < page_size → parar  // ultima pagina
        se max_rows e total >= max_rows → parar

        pagina += 1
    retornar todas_linhas
```

### Exemplos Python

```python
# Paginacao manual
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM", page_size=500)
all_rows = list(page.rows)
while page.has_next:
    page = client.fetch_next(page)
    all_rows.extend(page.rows)
    print(f"Buscadas {page.page_info.total_fetched} linhas ate agora...")

# Auto-paginacao com progresso
def on_page(result):
    pi = result.page_info
    print(f"Pagina {pi.page}: {pi.rows_returned} linhas ({pi.total_fetched} total)")

result = client.query_all(
    "SELECT * FROM AP_INVOICES_ALL ORDER BY INVOICE_ID",
    max_rows=10000,
    on_page=on_page,
)
```

> **Importante:** Sempre inclua `ORDER BY` para paginacao deterministica. Sem ele, o Oracle nao garante a ordem das linhas e linhas podem se deslocar entre paginas.

---

## Autenticacao

### Basic Auth

```python
from fusion_query import FusionClient, BasicAuth

# Atalho
client = FusionClient("https://...", "user", "pass")

# Explicito
client = FusionClient("https://...", auth=BasicAuth("user", "pass"))
```

### OAuth2 (credenciais de cliente via IDCS)

```python
from fusion_query import FusionClient, OAuth2Auth

auth = OAuth2Auth(
    token_url="https://idcs-xxx.identity.oraclecloud.com/oauth2/v1/token",
    client_id="abc123",
    client_secret="secret",
)
client = FusionClient("https://...", auth=auth)
```

---

## Deploy do relatorio proxy

O relatorio proxy e **implantado automaticamente no primeiro uso** — nao requer configuracao manual.

### Como funciona o auto-deploy

1. No primeiro `query()` ou `test_connection()`, o fusion-query verifica se o relatorio proxy existe
2. Se nao encontrado, implanta na sua **pasta pessoal do BIP** (`/~usuario/FusionQuery/v1/`)
3. Qualquer usuario autenticado pode escrever na sua propria pasta `~/` — nao requer papel de Administrador BI
4. Usa a API SOAP para implantacao (funciona em todas as instancias incluindo OCS)

### Deploy compartilhado (opcional)

Para implantar em uma pasta compartilhada acessivel por todos os usuarios:

```bash
fusion-query setup --url https://xxxx.fa.us2.oraclecloud.com --user bi_admin
```

Isso implanta em `/Custom/FusionQuery/Proxy/v1/` que requer papel de **Administrador BI** mas e compartilhado entre todos os usuarios.

### Implantacao manual (se necessario)

1. Acesse o Oracle Fusion como Administrador BI
2. Navegue ate **Relatorios e Analises > Catalogo**
3. Crie a pasta: `/Shared Folders/Custom/FusionQuery/Proxy/v1/`
4. Crie um Data Model com parametro `P_B64_CONTENT` (String), data source `ApplicationDB_FSCM` e o PL/SQL mostrado acima
5. Crie um Relatorio referenciando o Data Model, formato CSV, delimitador `|`

---

## Limitacoes

- **1000 linhas por requisicao** — tratado transparentemente via paginacao
- **Somente leitura** — apenas SELECT (sem INSERT/UPDATE/DELETE/DDL)
- **Data source** — template usa `ApplicationDB_FSCM`; modifique para HCM/SCM
- **Timeouts** — consultas grandes podem expirar; ajuste com `timeout=`

## Contribuindo

Contribuicoes sao bem-vindas! Areas de interesse:

- **Driver Java** — wrapper compativel com JDBC para DBeaver, SQL Developer, etc.
- **Driver Rust** — cliente nativo de alta performance
- **JavaScript/TypeScript** — suporte Node.js e browser
- **Driver Go** — para ferramentas cloud-native

Veja a [secao de Arquitetura](README.md#architecture-for-driver-implementors) para a especificacao do protocolo.

## Licenca

MIT

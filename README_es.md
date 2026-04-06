<p align="center">
  <img src="docs/logo.svg" width="120" alt="fusion-query logo">
</p>

<h1 align="center">fusion-query</h1>

<p align="center">
  <strong>Motor universal de consultas SQL para Oracle Fusion Cloud via BI Publisher</strong>
</p>

<p align="center">
  <a href="README.md">English</a> |
  <a href="README_pt-BR.md">Portugues</a>
</p>

---

Ejecute consultas SQL arbitrarias contra Oracle Fusion Cloud (ERP, HCM, SCM) usando **solo URL, usuario y contrasena**. Sin JDBC, sin VPN, sin acceso directo a la base de datos.

## Como funciona

```
SQL → gzip → base64 → HTTP POST a BI Publisher → PL/SQL REF CURSOR → CSV → filas parseadas
```

fusion-query utiliza Oracle BI Publisher con un reporte proxy ligero. El reporte contiene un bloque PL/SQL que recibe el SQL comprimido, lo ejecuta via REF CURSOR contra la base de datos Fusion y devuelve los resultados en CSV delimitado por pipe. Funciona con las APIs REST y SOAP, seleccionando automaticamente el mejor transporte para su instancia.

```sql
-- PL/SQL dentro del Data Model del reporte proxy:
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

## Instalacion

```bash
pip install fusion-query
```

Con extras:
```bash
pip install fusion-query[server]   # Servidor REST API (FastAPI + Uvicorn)
pip install fusion-query[cli]      # Formato de tabla (Rich)
pip install fusion-query[all]      # Todo
```

---

## Inicio rapido

### Solo conecte y consulte — sin configuracion previa

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "usuario", "contrasena")

result = client.query("SELECT USER_NAME, EMAIL_ADDRESS FROM PER_USERS")
for row in result.rows:
    print(row["USER_NAME"], row["EMAIL_ADDRESS"])
```

El reporte proxy se **despliega automaticamente** en su carpeta personal de BIP (`/~usuario/FusionQuery/`) en el primer uso. No requiere rol de Administrador BI — cualquier usuario autenticado puede comenzar a consultar inmediatamente.

---

## Tres formas de uso

### Biblioteca Python

```python
from fusion_query import FusionClient

client = FusionClient("https://xxxx.fa.us2.oraclecloud.com", "user", "pass")

# Pagina unica (hasta 1000 filas)
result = client.query("SELECT * FROM PER_USERS")

# Auto-paginacion (todas las filas)
result = client.query_all("SELECT * FROM PER_USERS ORDER BY USER_ID", max_rows=5000)

# Paginacion manual
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM")
while page.has_next:
    page = client.fetch_next(page)
```

### CLI

```bash
# Ejecutar consulta
fusion-query query --url https://... --user admin "SELECT SYSDATE FROM DUAL"

# Salida JSON
fusion-query query --url ... --user admin -f json "SELECT * FROM PER_USERS"

# Obtener todas las filas (auto-paginacion)
fusion-query query --url ... --user admin --all --max-rows 5000 \
  "SELECT * FROM PER_USERS ORDER BY USER_ID"

# Probar conexion
fusion-query test --url https://... --user admin
```

### API REST (cualquier lenguaje: Java, Rust, JS, Go, etc.)

```bash
fusion-query serve --port 8000
# Documentacion de la API en http://localhost:8000/docs
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

# Auto-paginacion
curl -X POST http://localhost:8000/query/all \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM PER_USERS ORDER BY USER_ID",
       "connection":"prod", "max_rows":5000}'
```

---

## Paginacion

### El problema

Oracle BI Publisher impone un **limite de ~1000 filas** por ejecucion de reporte. Las consultas que devuelven mas son silenciosamente truncadas.

### La solucion

fusion-query envuelve su SQL con `OFFSET ... FETCH NEXT ... ROWS ONLY` y realiza multiples solicitudes HTTP para obtener todos los datos de forma transparente en paginas.

### PageInfo — contrato universal de paginacion

Cada respuesta incluye un objeto `page_info`. **Todos los drivers (Python, Java, Rust, JS) deben implementar esta misma estructura:**

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

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `page` | int | Numero de pagina actual (base 0) |
| `page_size` | int | Filas solicitadas por pagina (max 1000) |
| `offset` | int | OFFSET SQL usado en esta pagina |
| `rows_returned` | int | Filas reales en esta pagina |
| `has_next` | bool | `true` si `rows_returned == page_size` (probablemente hay mas datos) |
| `total_fetched` | int | Total acumulado de filas en todas las paginas |
| `max_rows` | int/null | Limite superior definido por el llamador (null = ilimitado) |
| `exhausted` | bool | `true` si no hay mas datos o se alcanzo max_rows |

### Algoritmo de paginacion (para implementadores de drivers)

```
funcion query_all(sql, page_size=1000, max_rows=None):
    todas_filas = []
    pagina = 0

    bucle:
        sql_paginado = envolver_con_offset(sql, pagina * page_size, page_size)
        codificado = base64(gzip(sql_paginado))
        respuesta = HTTP_POST(url_bip, {P_B64_CONTENT: codificado})
        filas = parse_csv(base64_decode(respuesta.reportBytes))

        todas_filas.agregar(filas)

        si len(filas) < page_size → detener  // ultima pagina
        si max_rows y total >= max_rows → detener

        pagina += 1
    retornar todas_filas
```

### Ejemplos Python

```python
# Paginacion manual
page = client.query("SELECT * FROM GL_JE_LINES ORDER BY JE_LINE_NUM", page_size=500)
all_rows = list(page.rows)
while page.has_next:
    page = client.fetch_next(page)
    all_rows.extend(page.rows)
    print(f"Obtenidas {page.page_info.total_fetched} filas hasta ahora...")

# Auto-paginacion con progreso
def on_page(result):
    pi = result.page_info
    print(f"Pagina {pi.page}: {pi.rows_returned} filas ({pi.total_fetched} total)")

result = client.query_all(
    "SELECT * FROM AP_INVOICES_ALL ORDER BY INVOICE_ID",
    max_rows=10000,
    on_page=on_page,
)
```

> **Importante:** Siempre incluya `ORDER BY` para paginacion deterministica. Sin el, Oracle no garantiza el orden de las filas y estas pueden desplazarse entre paginas.

---

## Autenticacion

### Basic Auth

```python
from fusion_query import FusionClient, BasicAuth

# Atajo
client = FusionClient("https://...", "user", "pass")

# Explicito
client = FusionClient("https://...", auth=BasicAuth("user", "pass"))
```

### OAuth2 (credenciales de cliente via IDCS)

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

## Despliegue del reporte proxy

El reporte proxy se **despliega automaticamente en el primer uso** — no requiere configuracion manual.

### Como funciona el auto-deploy

1. En el primer `query()` o `test_connection()`, fusion-query verifica si el reporte proxy existe
2. Si no se encuentra, lo despliega en su **carpeta personal de BIP** (`/~usuario/FusionQuery/v1/`)
3. Cualquier usuario autenticado puede escribir en su propia carpeta `~/` — no requiere rol de Administrador BI
4. Usa la API SOAP para el despliegue (funciona en todas las instancias incluyendo OCS)

### Despliegue compartido (opcional)

Para desplegar en una carpeta compartida accesible por todos los usuarios:

```bash
fusion-query setup --url https://xxxx.fa.us2.oraclecloud.com --user bi_admin
```

Esto despliega en `/Custom/FusionQuery/Proxy/v1/` que requiere rol de **Administrador BI** pero es compartido entre todos los usuarios.

### Despliegue manual (si es necesario)

1. Ingrese a Oracle Fusion como Administrador BI
2. Navegue a **Reportes y Analisis > Catalogo**
3. Cree la carpeta: `/Shared Folders/Custom/FusionQuery/Proxy/v1/`
4. Cree un Data Model con parametro `P_B64_CONTENT` (String), data source `ApplicationDB_FSCM` y el PL/SQL mostrado arriba
5. Cree un Reporte referenciando el Data Model, formato CSV, delimitador `|`

---

## Limitaciones

- **1000 filas por solicitud** — manejado transparentemente via paginacion
- **Solo lectura** — solo SELECT (sin INSERT/UPDATE/DELETE/DDL)
- **Data source** — la plantilla usa `ApplicationDB_FSCM`; modifique para HCM/SCM
- **Timeouts** — consultas grandes pueden expirar; ajuste con `timeout=`

## Contribuir

Las contribuciones son bienvenidas! Areas de interes:

- **Driver Java** — wrapper compatible con JDBC para DBeaver, SQL Developer, etc.
- **Driver Rust** — cliente nativo de alto rendimiento
- **JavaScript/TypeScript** — soporte Node.js y navegador
- **Driver Go** — para herramientas cloud-native

Vea la [seccion de Arquitectura](README.md#architecture-for-driver-implementors) para la especificacion del protocolo.

## Licencia

MIT

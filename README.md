# Retail Data Quality Pipeline

## 1. Projekt célja

A projekt célja egy end-to-end adatfeldolgozó pipeline megvalósítása, amely:

- több forrásból származó CSV adatokat olvas be
- adatminőségi (data quality) szabályokat alkalmaz
- a hibás rekordokat elkülöníti
- a tiszta adatokat feldolgozza és tárolja
- KPI (key performance indicator) mutatókat számol
- automatizáltan futtatható CI/CD pipeline segítségével

A pipeline célja, hogy bemutassa egy **data engineering rendszer alapjait** valós üzleti logika mentén.

---

## 2. Architektúra

A pipeline fő lépései:

```
Raw CSV → Ingestion → Validation → Transformation → Split (clean/rejected)
       → SQL layer (SQLite) → KPI számítás → Reporting
       → (opcionálisan) Cloud storage → CI/CD (Jenkins)
```

### Fő komponensek:

- **Source**
  - `data/raw/*.csv`

- **Ingestion**
  - `ingest.py` – fájlok betöltése és schema ellenőrzés

- **Validation**
  - `validate.py` – adatminőségi szabályok alkalmazása

- **Transformation**
  - `transform.py` – típuskonverziók és adat előkészítés

- **Split layer**
  - clean / rejected adatok szétválasztása

- **SQL serving layer**
  - SQLite adatbázis (`load_to_sqlite.py`)

- **Reporting / KPI**
  - SQL view-k és aggregációk

- **Spark pipeline**
  - `pyspark_jobs/spark_pipeline.py`

- **Cloud storage**
  - Azure Data Lake Storage (feltöltés-letöltés script)

- **CI/CD**
  - Jenkins pipeline (`Jenkinsfile`)

---

## 3. Használt technológiák

- **Operációs rendszer**
  - Ubuntu Linux

- **Fejlesztői környezet**
  - VS Code

- **Programozás**
  - Python

- **Adatfeldolgozás**
  - pandas
  - PySpark

- **Adatbázis**
  - SQLite

- **Cloud**
  - Azure Storage (ADLS Gen2)

- **CI/CD**
  - Jenkins (Dockerben futtatva)

- **Verziókezelés**
  - Git + GitHub

---

## 4. Futtatás

### 4.1. Környezet létrehozása

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

### 4.2. Pipeline futtatása (pandas verzió)

```bash
python -m src.pipeline
```

---

### 4.3. SQLite betöltés és KPI-k

```bash
python -m src.load_to_sqlite
```

---

### 4.4. PySpark pipeline futtatása

```bash
python pyspark_jobs/spark_pipeline.py
```

---

### 4.5. Tesztek futtatása

```bash
pytest -v
```

---

### 4.6. Jenkins pipeline

A pipeline automatizált futtatása Jenkins segítségével történik:

- repository checkout
- Python környezet létrehozása
- dependency install
- tesztek futtatása
- pipeline futtatása
- SQLite betöltés
- artifact mentés

---

## 5. Data Quality szabályok

A szabályok konfigurálhatóak YAML fájlban (`configs/quality_rules.yaml`).

### Fő szabálytípusok:

- **Required mezők**
  - pl. `customer_id` nem lehet null vagy üres

- **Unique mezők**
  - pl. `order_id` egyedi

- **Dátum validáció**
  - pl. `order_date` helyes formátum

- **Numerikus határok**
  - pl. `quantity >= 1`
  - `total_amount >= 0`

- **Email validáció**
  - regex alapú ellenőrzés

- **Foreign key ellenőrzés**
  - orders.customer_id → customers.customer_id
  - orders.product_id → products.product_id

- **Severity**
  - `ERROR`
  - `WARNING`

---

## 6. Outputok

### Fájlok

- `data/processed/clean_*.csv`
- `data/rejected/rejected_*.csv`

---

### Logok

- `logs/*_validation_report.csv`
- `logs/quality_summary.csv`

---

### SQLite táblák

- `clean_orders`
- `clean_customers`
- `clean_products`

---

### KPI view-k

- `daily_sales_kpi`
- `category_sales_summary`
- `customer_country_summary`

---

### Spark output

- `data/processed/spark_*`
- `data/rejected/spark_*`

---

### Jenkins artifactok

A pipeline futása során generált fájlok archiválásra kerülnek Jenkinsben:

- clean / rejected CSV-k
- logok
- SQLite adatbázis

---

## 7. Fő tanulságok

### Miért fontos a validáció?

- hibás adatok torzítják az üzleti riportokat
- korai szűrés = megbízhatóbb rendszer

---

### Miért kell rejected layer?

- nem vesznek el a hibás rekordok
- visszakövethető az adatminőség
- auditálható rendszer

---

### Mikor elég pandas, mikor kell Spark?

| pandas | Spark |
|------|------|
| kis adat | nagy adat |
| gyors fejlesztés | skálázhatóság |
| egyszerű pipeline | distributed processing |

---

### Miért hasznos az automatizált tesztelés?

- gyors visszajelzés
- regressziók elkerülése
- stabil pipeline

---

### Miért fontos a CI/CD?

- automatizált futtatás
- konzisztens környezet
- gyors hibafelismerés

---

## Összegzés

A projekt egy teljes adatfeldolgozó pipeline-t valósít meg:

- adat betöltés → validáció → transzformáció → tárolás → riportolás
- pandas és PySpark implementációval
- konfigurálható data quality szabályokkal
- automatizált Jenkins pipeline-nal

Ez a struktúra jól modellezi egy valós data engineering rendszer működését.

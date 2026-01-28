# DEZoocamp 2026

Data Engineering bootcamp organizzato da [DataTalks.Club](https://datatalks.club/).

## Struttura del corso

- **01-docker-terraform/** - Docker, PostgreSQL, Terraform, Google Cloud setup
- **02-workflow-orchestration/** - Workflow orchestration con Kestra *(coming soon)*
- **03-data-warehouse/** - Data warehousing con BigQuery *(coming soon)*
- **04-analytics-engineering/** - Analytics engineering con dbt *(coming soon)*
- **05-batch/** - Batch processing con PySpark *(coming soon)*
- **06-streaming/** - Stream processing con Kafka e Flink *(coming soon)*
- **projects/** - Progetti finali *(coming soon)*

## Risorse

- [Corso ufficiale](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Community Slack](https://datatalks.club/slack.html)
- [FAQ del corso](https://docs.google.com/document/d/19bnYs80DwuUimHM65UV3sylsCn2j1vziPOwzBwQrebw/edit)

## Modulo 1: Docker e Terraform

### Contenuti
- Introduzione a Docker e containerizzazione
- PostgreSQL in Docker
- Ingestione dati NYC Taxi (CSV e Parquet)
- pgAdmin per gestione database
- Introduzione a Terraform
- Setup Google Cloud Platform (GCP)

### File principali
- `pipeline/ingest_data_new.py` - Script per ingestione dati Parquet
- `pipeline/docker-compose.yaml` - Configurazione Docker Compose
- `terraform/main.tf` - Configurazione Terraform per GCP

### Come eseguire

#### 1. Avvia il database PostgreSQL
```bash
docker-compose up -d pgdatabase
```

#### 2. Carica i dati NYC Taxi
```bash
docker run -it --network=pg-network taxi_ingest:v002 \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=pipeline-pgdatabase-1 \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_data_2025_11 \
  --year=2025 \
  --month=11 \
  --file-format=parquet
```

#### 3. Accedi a pgAdmin
```bash
docker-compose up -d pgadmin
```
Poi vai su http://localhost:8080

### Query SQL utili

#### Convertire datetime in date
```sql
SELECT tpep_pickup_datetime::date AS pickup_date
FROM yellow_taxi_data_2025_11
LIMIT 10;
```

#### Join con zone lookup
```sql
SELECT 
    dropoff_zone."Zone" AS dropoff_zone,
    SUM(trips."tip_amount") AS total_tips
FROM yellow_taxi_data_2025_11 AS trips
JOIN zones AS pickup_zone ON pickup_zone."LocationID" = trips."PULocationID"
JOIN zones AS dropoff_zone ON dropoff_zone."LocationID" = trips."DOLocationID"
WHERE trips."tpep_pickup_datetime"::date BETWEEN '2025-11-01' AND '2025-11-30'
AND pickup_zone."Zone" = 'East Harlem North'
GROUP BY dropoff_zone."Zone"
ORDER BY total_tips DESC;
```

## Note

### File sensibili
I seguenti file sono esclusi dal repository tramite `.gitignore`:
- `terraform/keys/*.json` - Credenziali Google Cloud
- `*.tfstate` - Stato Terraform
- File di ambiente e credenziali

### Troubleshooting

#### Docker network issues
Se il container non si connette al database:
```bash
docker network connect pg-network <nome-container-db>
```

#### GitHub secret scanning
Se GitHub blocca il push per file sensibili:
1. Rimuovi il file: `git rm --cached <file>`
2. Aggiungi al `.gitignore`
3. Commit e push

## Progresso

- [x] Module 1: Docker & Terraform
- [ ] Module 2: Workflow Orchestration
- [ ] Module 3: Data Warehouse
- [ ] Module 4: Analytics Engineering
- [ ] Module 5: Batch Processing
- [ ] Module 6: Stream Processing
- [ ] Final Project

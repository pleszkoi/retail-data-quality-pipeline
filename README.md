# Retail Data Quality Pipeline

A practice project for learning data engineering and data quality concepts using Python, SQL, PySpark, Azure-oriented design, Jenkins, and Azure DevOps.

## Planned scope
- Data ingestion from raw files
- Data quality validation
- Clean and rejected layers
- SQL-based KPI generation
- PySpark processing
- Azure storage integration
- CI/CD with Jenkins and Azure DevOps

## Tech stack
- Ubuntu Linux
- VS Code
- Python
- SQL
- PySpark
- Git

## SQL Layer
- Clean CSV outputs are loaded into SQLite
- SQL views are used to calculate KPI summaries
- Main outputs:
  - daily_sales_kpi
  - category_sales_summary
  - customer_country_summary

## Testing

```bash
pytest -v
```

Tests cover:

- validation logic
- YAML rule loading
- pipeline output generation
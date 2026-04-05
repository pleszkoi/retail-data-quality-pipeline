# Data Quality Rules

## Customers

- customer_id must not be null
- customer_id must be unique
- full_name must not be empty
- email should follow basic email format
- country must not be null
- registration_date must be a valid date

## Products

- product_id must not be null
- product_id must be unique
- price must be >= 0
- category should not be null

## Orders

- order_id must be unique
- customer_id must exist in customers
- product_id must exist in products
- quantity must be > 0
- total_amount must be >= 0
- order_date must be valid date

## Severity levels

- ERROR: breaks processing (e.g. missing ID, invalid foreign key)
- WARNING: data is usable but not ideal (e.g. bad email format)

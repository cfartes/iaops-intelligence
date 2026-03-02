CREATE TABLE IF NOT EXISTS iaops_gov.data_source_catalog (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    is_supported BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO iaops_gov.data_source_catalog (code, name, category, is_supported, notes)
VALUES
    ('postgresql', 'PostgreSQL', 'relational', TRUE, 'Banco relacional'),
    ('mysql', 'MySQL', 'relational', TRUE, 'Banco relacional'),
    ('sqlserver', 'SQL Server', 'relational', TRUE, 'Banco relacional'),
    ('oracle', 'Oracle', 'relational', TRUE, 'Banco relacional'),
    ('mongodb', 'MongoDB', 'nosql', TRUE, 'Documento NoSQL'),
    ('cassandra', 'Cassandra', 'nosql', TRUE, 'Wide-column NoSQL'),
    ('dynamodb', 'DynamoDB', 'nosql', TRUE, 'NoSQL gerenciado AWS'),
    ('snowflake', 'Snowflake', 'warehouse', TRUE, 'Data warehouse cloud'),
    ('bigquery', 'BigQuery', 'warehouse', TRUE, 'Data warehouse Google'),
    ('redshift', 'Redshift', 'warehouse', TRUE, 'Data warehouse AWS'),
    ('aws_s3', 'AWS S3', 'lake_storage', TRUE, 'Data lake/object storage'),
    ('azure_blob', 'Azure Blob Storage', 'lake_storage', TRUE, 'Data lake/object storage'),
    ('gcs', 'Google Cloud Storage', 'lake_storage', TRUE, 'Data lake/object storage'),
    ('power_bi', 'Power BI', 'bi_semantic', TRUE, 'Semantic model e datasets BI'),
    ('microsoft_fabric', 'Microsoft Fabric', 'lakehouse_semantic', TRUE, 'Lakehouse, Warehouse e semantic model')
ON CONFLICT (code) DO UPDATE SET
    name = EXCLUDED.name,
    category = EXCLUDED.category,
    is_supported = EXCLUDED.is_supported,
    notes = EXCLUDED.notes;
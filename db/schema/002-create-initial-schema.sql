-- You need to have executed this as a PostgreSQL super user for the database:
-- CREATE EXTENSION "uuid-ossp";

CREATE TABLE history_documents (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  schema text NOT NULL,
  classification jsonb NOT NULL,
  doc jsonb NOT NULL,
  source jsonb NOT NULL
);

CREATE TABLE latest_documents (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  schema text NOT NULL,
  classification jsonb NOT NULL,
  doc jsonb NOT NULL,
  source jsonb NOT NULL
);

CREATE INDEX idx_history_source ON history_documents USING gin (source jsonb_path_ops);
CREATE INDEX idx_history_classification ON history_documents (classification);
CREATE INDEX idx_latest_source ON latest_documents USING gin (source jsonb_path_ops);
CREATE INDEX idx_latest_classification ON latest_documents (classification);

INSERT INTO db_version (db_version_id, file_name, jira_issue)
VALUES (2, '002-create-initial-schema.sql', 'MV-136');

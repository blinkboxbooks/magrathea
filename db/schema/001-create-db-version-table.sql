CREATE TABLE db_version (
  db_version_id smallint PRIMARY KEY,
  applied_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  file_name varchar(100) NOT NULL,
  jira_issue varchar(12) NOT NULL
);

CREATE OR REPLACE FUNCTION update_applied_at_column()
RETURNS TRIGGER AS $$
BEGIN
  IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
    NEW.applied_at = now();
    RETURN NEW;
  ELSE
    RETURN OLD;
  END IF;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_db_version_applied_at BEFORE UPDATE
  ON db_version FOR EACH ROW EXECUTE PROCEDURE update_applied_at_column();

INSERT INTO db_version (db_version_id, file_name, jira_issue)
VALUES (1, '001-create-db-version-table.sql', 'MV-136');

-- The transcript's incremental test, for the dev source schema. Run it in the SQL editor, section by section.

-- 1. Checks BEFORE (run on bronze): customer exists, claim is 'Total Loss', policy does not exist yet.
SELECT * FROM e2e_dev.dev_keqingli1129_bronze.customer WHERE customer_id = 'C007000';
SELECT claim_no, incident_severity FROM e2e_dev.dev_keqingli1129_bronze.claim WHERE claim_no = 'CLM00000003';
SELECT * FROM e2e_dev.dev_keqingli1129_bronze.policy WHERE policy_no = 'POL9999001';

-- 2. Changes in the SOURCE (plays the DataGrip part).
INSERT INTO e2e_dev.dev_keqingli1129_source.policy VALUES
  ('POL9999001', 'C000001', 'CHS999001', 'Tesla', 'Model Y', 2026, 'COMPREHENSIVE', 1899.00, 500,
   DATE'2026-09-01', DATE'2027-09-01');

UPDATE e2e_dev.dev_keqingli1129_source.claim
SET incident_severity = 'Minor Damage', updated_at = current_timestamp()
WHERE claim_no = 'CLM00000003';

DELETE FROM e2e_dev.dev_keqingli1129_source.customer WHERE customer_id = 'C007000';

-- 3. Now run the pipeline cdc_ingestion, then re-run the checks in section 1:
--    policy row appears, claim severity is 'Minor Damage', customer returns no rows.

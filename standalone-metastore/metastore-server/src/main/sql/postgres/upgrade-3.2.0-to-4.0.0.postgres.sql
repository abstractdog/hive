SELECT 'Upgrading MetaStore schema from 3.2.0 to 4.0.0';

-- HIVE-19416
ALTER TABLE "TBLS" ADD "WRITE_ID" bigint DEFAULT 0;
ALTER TABLE "PARTITIONS" ADD "WRITE_ID" bigint DEFAULT 0;


-- HIVE-20793
ALTER TABLE "WM_RESOURCEPLAN" ADD "NS" character varying(128);
UPDATE "WM_RESOURCEPLAN" SET "NS" = 'default' WHERE "NS" IS NULL;
ALTER TABLE "WM_RESOURCEPLAN" DROP CONSTRAINT "UNIQUE_WM_RESOURCEPLAN";
ALTER TABLE ONLY "WM_RESOURCEPLAN" ADD CONSTRAINT "UNIQUE_WM_RESOURCEPLAN" UNIQUE ("NS", "NAME");

-- These lines need to be last.  Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.0.0', "VERSION_COMMENT"='Hive release version 4.0.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 3.2.0 to 4.0.0';


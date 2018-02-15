SELECT 'Upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';

--SOURCE 041-HIVE-16556.mysql.sql;
--
-- Table structure for table METASTORE_DB_PROPERTIES
--
CREATE TABLE IF NOT EXISTS `METASTORE_DB_PROPERTIES` (
  `PROPERTY_KEY` varchar(255) NOT NULL,
  `PROPERTY_VALUE` varchar(1000) NOT NULL,
  `DESCRIPTION` varchar(1000),
 PRIMARY KEY(`PROPERTY_KEY`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--SOURCE 042-HIVE-16575.mysql.sql;
CREATE INDEX `CONSTRAINTS_CONSTRAINT_TYPE_INDEX` ON KEY_CONSTRAINTS (`CONSTRAINT_TYPE`) USING BTREE;

--SOURCE 043-HIVE-16922.mysql.sql;
UPDATE SERDE_PARAMS
SET PARAM_KEY='collection.delim'
WHERE PARAM_KEY='colelction.delim';

--SOURCE 044-HIVE-16997.mysql.sql;
ALTER TABLE PART_COL_STATS ADD COLUMN BIT_VECTOR BLOB;
ALTER TABLE TAB_COL_STATS ADD COLUMN BIT_VECTOR BLOB;

--SOURCE 045-HIVE-16886.mysql.sql;
INSERT INTO `NOTIFICATION_SEQUENCE` (`NNI_ID`, `NEXT_EVENT_ID`) SELECT * from (select 1 as `NNI_ID`, 1 as `NOTIFICATION_SEQUENCE`) a WHERE (SELECT COUNT(*) FROM `NOTIFICATION_SEQUENCE`) = 0;

--SOURCE 046-HIVE-17566.mysql.sql;
CREATE TABLE IF NOT EXISTS WM_RESOURCEPLAN (
    `RP_ID` bigint(20) NOT NULL,
    `NAME` varchar(128) NOT NULL,
    `QUERY_PARALLELISM` int(11),
    `STATUS` varchar(20) NOT NULL,
    `DEFAULT_POOL_ID` bigint(20),
    PRIMARY KEY (`RP_ID`),
    UNIQUE KEY `UNIQUE_WM_RESOURCEPLAN` (`NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS WM_POOL
(
    `POOL_ID` bigint(20) NOT NULL,
    `RP_ID` bigint(20) NOT NULL,
    `PATH` varchar(767) NOT NULL,
    `ALLOC_FRACTION` DOUBLE,
    `QUERY_PARALLELISM` int(11),
    `SCHEDULING_POLICY` varchar(767),
    PRIMARY KEY (`POOL_ID`),
    UNIQUE KEY `UNIQUE_WM_POOL` (`RP_ID`, `PATH`),
    CONSTRAINT `WM_POOL_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

ALTER TABLE `WM_RESOURCEPLAN` ADD CONSTRAINT `WM_RESOURCEPLAN_FK1` FOREIGN KEY (`DEFAULT_POOL_ID`) REFERENCES `WM_POOL`(`POOL_ID`);

CREATE TABLE IF NOT EXISTS WM_TRIGGER
(
    `TRIGGER_ID` bigint(20) NOT NULL,
    `RP_ID` bigint(20) NOT NULL,
    `NAME` varchar(128) NOT NULL,
    `TRIGGER_EXPRESSION` varchar(1024),
    `ACTION_EXPRESSION` varchar(1024),
    `IS_IN_UNMANAGED` bit(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (`TRIGGER_ID`),
    UNIQUE KEY `UNIQUE_WM_TRIGGER` (`RP_ID`, `NAME`),
    CONSTRAINT `WM_TRIGGER_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS WM_POOL_TO_TRIGGER
(
    `POOL_ID` bigint(20) NOT NULL,
    `TRIGGER_ID` bigint(20) NOT NULL,
    PRIMARY KEY (`POOL_ID`, `TRIGGER_ID`),
    CONSTRAINT `WM_POOL_TO_TRIGGER_FK1` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`),
    CONSTRAINT `WM_POOL_TO_TRIGGER_FK2` FOREIGN KEY (`TRIGGER_ID`) REFERENCES `WM_TRIGGER` (`TRIGGER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS WM_MAPPING
(
    `MAPPING_ID` bigint(20) NOT NULL,
    `RP_ID` bigint(20) NOT NULL,
    `ENTITY_TYPE` varchar(128) NOT NULL,
    `ENTITY_NAME` varchar(128) NOT NULL,
    `POOL_ID` bigint(20),
    `ORDERING` int,
    PRIMARY KEY (`MAPPING_ID`),
    UNIQUE KEY `UNIQUE_WM_MAPPING` (`RP_ID`, `ENTITY_TYPE`, `ENTITY_NAME`),
    CONSTRAINT `WM_MAPPING_FK1` FOREIGN KEY (`RP_ID`) REFERENCES `WM_RESOURCEPLAN` (`RP_ID`),
    CONSTRAINT `WM_MAPPING_FK2` FOREIGN KEY (`POOL_ID`) REFERENCES `WM_POOL` (`POOL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

UPDATE VERSION SET SCHEMA_VERSION='3.0.0', VERSION_COMMENT='Hive release version 3.0.0' where VER_ID=1;
SELECT 'Finished upgrading MetaStore schema from 2.3.0 to 3.0.0' AS ' ';

-- 048-HIVE-14498
CREATE TABLE IF NOT EXISTS `MV_CREATION_METADATA` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `DB_NAME` varchar(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TBL_NAME` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `TXN_LIST` TEXT DEFAULT NULL,
  PRIMARY KEY (`MV_CREATION_METADATA_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE INDEX MV_UNIQUE_TABLE ON MV_CREATION_METADATA (TBL_NAME, DB_NAME) USING BTREE;

CREATE TABLE IF NOT EXISTS `MV_TABLES_USED` (
  `MV_CREATION_METADATA_ID` bigint(20) NOT NULL,
  `TBL_ID` bigint(20) NOT NULL,
  CONSTRAINT `MV_TABLES_USED_FK1` FOREIGN KEY (`MV_CREATION_METADATA_ID`) REFERENCES `MV_CREATION_METADATA` (`MV_CREATION_METADATA_ID`),
  CONSTRAINT `MV_TABLES_USED_FK2` FOREIGN KEY (`TBL_ID`) REFERENCES `TBLS` (`TBL_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

ALTER TABLE `COMPLETED_TXN_COMPONENTS` ADD `CTC_TIMESTAMP` timestamp;

UPDATE `COMPLETED_TXN_COMPONENTS` SET `CTC_TIMESTAMP` = CURRENT_TIMESTAMP;

ALTER TABLE `COMPLETED_TXN_COMPONENTS` MODIFY COLUMN `CTC_TIMESTAMP` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

CREATE INDEX COMPLETED_TXN_COMPONENTS_IDX ON COMPLETED_TXN_COMPONENTS (CTC_DATABASE, CTC_TABLE, CTC_PARTITION) USING BTREE;

-- 049-HIVE-18489.mysql.sql
UPDATE FUNC_RU
  SET RESOURCE_URI = CONCAT('s3a', SUBSTR(RESOURCE_URI, 4, LENGTH(RESOURCE_URI)))
  WHERE RESOURCE_URI LIKE 's3n://%' ;

UPDATE SKEWED_COL_VALUE_LOC_MAP
  SET LOCATION = CONCAT('s3a', SUBSTR(LOCATION, 4, LENGTH(LOCATION)))
  WHERE LOCATION LIKE 's3n://%' ;

UPDATE SDS
  SET LOCATION = CONCAT('s3a', SUBSTR(LOCATION, 4, LENGTH(LOCATION)))
  WHERE LOCATION LIKE 's3n://%' ;

UPDATE DBS
  SET DB_LOCATION_URI = CONCAT('s3a', SUBSTR(DB_LOCATION_URI, 4, LENGTH(DB_LOCATION_URI)))
  WHERE DB_LOCATION_URI LIKE 's3n://%' ;

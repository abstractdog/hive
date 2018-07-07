/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline.schematool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.PostgresCommandParser;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaTool {

  /**
   * Test script formatting
   */
  @Test
  public void testScripts() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "/*!1234 this is comment code like mysql */;",
      "CREATE TABLE fooTab(id INTEGER);",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String resultScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "DROP TABLE footab",
    };
    String expectedSQL = StringUtils.join(resultScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForDerby() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "-- this is a comment ",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is also a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " -- this is another comment ",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "RUN '" + generateTestScript(childTestScript1).getName() + "';",
        "DROP TABLE " + parentTab + ";",
        "RUN '" + generateTestScript(childTestScript2).getName() + "';",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("derby", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("RUN"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForMySQL() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "/* this is a comment code */",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "/* this is a special exec code */;",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " /* this is special exec code */;",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "SOURCE " + generateTestScript(childTestScript1).getName() + ";",
        "DROP TABLE " + parentTab + ";",
        "SOURCE " + generateTestScript(childTestScript2).getName() + ";",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("mysql", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("RUN"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   */
  @Test
  public void testScriptWithDelimiter() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "DELIMITER $$",
      "/*!1234 this is comment code like mysql */$$",
      "CREATE TABLE fooTab(id INTEGER)$$",
      "CREATE PROCEDURE fooProc()",
      "SELECT * FROM fooTab;",
      "CALL barProc();",
      "END PROCEDURE$$",
      "DELIMITER ;",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String resultScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "CREATE PROCEDURE fooProc()" + " " +
      "SELECT * FROM fooTab;" + " " +
      "CALL barProc();" + " " +
      "END PROCEDURE",
      "DROP TABLE footab",
    };
    String expectedSQL = StringUtils.join(resultScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql", false);
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test script formatting
   */
  @Test
  public void testScriptMultiRowComment() throws Exception {
    String testScript[] = {
        "-- this is a comment",
      "DROP TABLE IF EXISTS fooTab;",
      "DELIMITER $$",
      "/*!1234 this is comment code like mysql */$$",
      "CREATE TABLE fooTab(id INTEGER)$$",
      "DELIMITER ;",
      "/* multiline comment started ",
      " * multiline comment continue",
      " * multiline comment ended */",
      "DROP TABLE footab;",
      "-- ending comment"
    };
    String parsedScript[] = {
      "DROP TABLE IF EXISTS fooTab",
      "/*!1234 this is comment code like mysql */",
      "CREATE TABLE fooTab(id INTEGER)",
      "DROP TABLE footab",
    };

    String expectedSQL = StringUtils.join(parsedScript, System.getProperty("line.separator")) +
        System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    NestedScriptParser testDbParser = HiveSchemaHelper.getDbCommandParser("mysql", false);
    String flattenedSql = testDbParser.buildCommand(testScriptFile.getParentFile().getPath(),
        testScriptFile.getName());

    Assert.assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test nested script formatting
   */
  @Test
  public void testNestedScriptsForOracle() throws Exception {
    String childTab1 = "childTab1";
    String childTab2 = "childTab2";
    String parentTab = "fooTab";

    String childTestScript1[] = {
      "-- this is a comment ",
      "DROP TABLE IF EXISTS " + childTab1 + ";",
      "CREATE TABLE " + childTab1 + "(id INTEGER);",
      "DROP TABLE " + childTab1 + ";"
    };
    String childTestScript2[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS " + childTab2 + ";",
        "CREATE TABLE " + childTab2 + "(id INTEGER);",
        "-- this is also a comment",
        "DROP TABLE " + childTab2 + ";"
    };

    String parentTestScript[] = {
        " -- this is a comment",
        "DROP TABLE IF EXISTS " + parentTab + ";",
        " -- this is another comment ",
        "CREATE TABLE " + parentTab + "(id INTEGER);",
        "@" + generateTestScript(childTestScript1).getName() + ";",
        "DROP TABLE " + parentTab + ";",
        "@" + generateTestScript(childTestScript2).getName() + ";",
        "--ending comment ",
      };

    File testScriptFile = generateTestScript(parentTestScript);
    String flattenedSql = HiveSchemaHelper.getDbCommandParser("oracle", false)
        .buildCommand(testScriptFile.getParentFile().getPath(),
            testScriptFile.getName());
    Assert.assertFalse(flattenedSql.contains("@"));
    Assert.assertFalse(flattenedSql.contains("comment"));
    Assert.assertTrue(flattenedSql.contains(childTab1));
    Assert.assertTrue(flattenedSql.contains(childTab2));
    Assert.assertTrue(flattenedSql.contains(parentTab));
  }

  /**
   * Test script formatting
   */
  @Test
  public void testPostgresFilter() throws Exception {
    String testScript[] = {
        "-- this is a comment",
        "DROP TABLE IF EXISTS fooTab;",
        HiveSchemaHelper.PostgresCommandParser.POSTGRES_STANDARD_STRINGS_OPT + ";",
        "CREATE TABLE fooTab(id INTEGER);",
        "DROP TABLE footab;",
        "-- ending comment"
    };

    String expectedScriptWithOptionPresent[] = {
        "DROP TABLE IF EXISTS fooTab",
        HiveSchemaHelper.PostgresCommandParser.POSTGRES_STANDARD_STRINGS_OPT,
        "CREATE TABLE fooTab(id INTEGER)",
        "DROP TABLE footab",
    };

    NestedScriptParser noDbOptParser = HiveSchemaHelper
        .getDbCommandParser("postgres", false);
    String expectedSQL = StringUtils.join(
        expectedScriptWithOptionPresent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    File testScriptFile = generateTestScript(testScript);
    String flattenedSql = noDbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    Assert.assertEquals(expectedSQL, flattenedSql);

    String expectedScriptWithOptionAbsent[] = {
        "DROP TABLE IF EXISTS fooTab",
        "CREATE TABLE fooTab(id INTEGER)",
        "DROP TABLE footab",
    };

    NestedScriptParser dbOptParser = HiveSchemaHelper.getDbCommandParser(
        "postgres",
        PostgresCommandParser.POSTGRES_SKIP_STANDARD_STRINGS_DBOPT,
        null, null, null, null, false);
    expectedSQL = StringUtils.join(
        expectedScriptWithOptionAbsent, System.getProperty("line.separator")) +
            System.getProperty("line.separator");
    testScriptFile = generateTestScript(testScript);
    flattenedSql = dbOptParser.buildCommand(
        testScriptFile.getParentFile().getPath(), testScriptFile.getName());
    assertEquals(expectedSQL, flattenedSql);
  }

  /**
   * Test validate uri of locations
   * @throws Exception
   */
  public void testValidateLocations() throws Exception {
    execute(new HiveSchemaToolTaskInit(), "-initSchema");
    URI defaultRoot = new URI("hdfs://myhost.com:8020");
    URI defaultRoot2 = new URI("s3://myhost2.com:8888");
    //check empty DB
    boolean isValid = validator.validateLocations(conn, null);
    assertTrue(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    assertTrue(isValid);

 // Test valid case
    String[] scripts = new String[] {
         "insert into CTLGS values(3, 'test_cat_2', 'description', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb')",
         "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'test_cat_2')",
         "insert into DBS values(7, 'db with bad port', 'hdfs://myhost.com:8020/', 'haDB', 'public', 'role', 'test_cat_2')",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (3,null,'org.apache.hadoop.mapred.TextInputFormat','N',null,-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
         "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4000,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n')",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3 ,1435255431,2,0 ,'hive',0,3,'myView','VIRTUAL_VIEW','select a.col1,a.col2 from foo','select * from foo','n')",
         "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4012 ,1435255431,7,0 ,'hive',0,4000,'mytal4012','MANAGED_TABLE',NULL,NULL,'n')",
         "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2)",
         "insert into SKEWED_STRING_LIST values(1)",
         "insert into SKEWED_STRING_LIST values(2)",
         "insert into SKEWED_COL_VALUE_LOC_MAP values(1,1,'hdfs://myhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/')",
         "insert into SKEWED_COL_VALUE_LOC_MAP values(2,2,'s3://myhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/')"
       };
    File scriptFile = generateTestScript(scripts);
    schemaTool.runBeeLine(scriptFile.getPath());
    isValid = validator.validateLocations(conn, null);
    assertTrue(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    assertTrue(isValid);
    scripts = new String[] {
        "delete from SKEWED_COL_VALUE_LOC_MAP",
        "delete from SKEWED_STRING_LIST",
        "delete from PARTITIONS",
        "delete from TBLS",
        "delete from SDS",
        "delete from DBS",
        "insert into DBS values(2, 'my db', '/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'test_cat_2')",
        "insert into DBS values(4, 'my db2', 'hdfs://myhost.com:8020', '', 'public', 'role', 'test_cat_2')",
        "insert into DBS values(6, 'db with bad port', 'hdfs://myhost.com:8020:', 'zDB', 'public', 'role', 'test_cat_2')",
        "insert into DBS values(7, 'db with bad port', 'hdfs://mynameservice.com/', 'haDB', 'public', 'role', 'test_cat_2')",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://yourhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','file:///user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (3000,null,'org.apache.hadoop.mapred.TextInputFormat','N','yourhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4000,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4001,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4003,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4004,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (4002,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (5000,null,'org.apache.hadoop.mapred.TextInputFormat','N','file:///user/admin/2016_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3000 ,1435255431,2,0 ,'hive',0,3000,'mytal3000','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4011 ,1435255431,4,0 ,'hive',0,4001,'mytal4011','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4012 ,1435255431,4,0 ,'hive',0,4002,'','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4013 ,1435255431,4,0 ,'hive',0,4003,'mytal4013','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (4014 ,1435255431,2,0 ,'hive',0,4003,'','MANAGED_TABLE',NULL,NULL,'n')",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4001, 1441402388,0, 'd1=1/d2=4001',4001,4011)",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4002, 1441402388,0, 'd1=1/d2=4002',4002,4012)",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4003, 1441402388,0, 'd1=1/d2=4003',4003,4013)",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(4004, 1441402388,0, 'd1=1/d2=4004',4004,4014)",
        "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(5000, 1441402388,0, 'd1=1/d2=5000',5000,2)",
        "insert into SKEWED_STRING_LIST values(1)",
        "insert into SKEWED_STRING_LIST values(2)",
        "insert into SKEWED_COL_VALUE_LOC_MAP values(1,1,'hdfs://yourhost.com:8020/user/hive/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/')",
        "insert into SKEWED_COL_VALUE_LOC_MAP values(2,2,'file:///user/admin/warehouse/mytal/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/')"
    };
    scriptFile = generateTestScript(scripts);
    schemaTool.runBeeLine(scriptFile.getPath());
    isValid = validator.validateLocations(conn, null);
    assertFalse(isValid);
    isValid = validator.validateLocations(conn, new URI[] {defaultRoot, defaultRoot2});
    assertFalse(isValid);
  }

  public void testHiveMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    execute(new HiveSchemaToolTaskInit(), "-initSchemaTo 3.0.0");
    validateMetastoreDbPropertiesTable();
  }

  public void testMetastoreDbPropertiesAfterUpgrade() throws HiveMetaException, IOException {
    execute(new HiveSchemaToolTaskInit(), "-initSchemaTo 2.0.0");
    execute(new HiveSchemaToolTaskUpgrade(), "-upgradeSchema");
    validateMetastoreDbPropertiesTable();
  }

  private File generateTestScript(String [] stmts) throws IOException {
    File testScriptFile = File.createTempFile("schematest", ".sql");
    testScriptFile.deleteOnExit();
    FileWriter fstream = new FileWriter(testScriptFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);
    for (String line: stmts) {
      out.write(line);
      out.newLine();
    }
    out.close();
    return testScriptFile;
  }

  private void validateMetastoreDbPropertiesTable() throws HiveMetaException, IOException {
    boolean isValid = (boolean) validator.validateSchemaTables(conn);
    assertTrue(isValid);
    // adding same property key twice should throw unique key constraint violation exception
    String[] scripts = new String[] {
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-1', 'dummy uuid 1')",
        "insert into METASTORE_DB_PROPERTIES values ('guid', 'test-uuid-2', 'dummy uuid 2')", };
    File scriptFile = generateTestScript(scripts);
    Exception ex = null;
    try {
      schemaTool.runBeeLine(scriptFile.getPath());
    } catch (Exception iox) {
      ex = iox;
    }
    assertTrue(ex != null && ex instanceof IOException);
  }
  /**
   * Write out a dummy pre-upgrade script with given SQL statement.
   */
  private String writeDummyPreUpgradeScript(int index, String upgradeScriptName,
      String sql) throws Exception {
    String preUpgradeScript = "pre-" + index + "-" + upgradeScriptName;
    String dummyPreScriptPath = System.getProperty("test.tmp.dir", "target/tmp") +
        File.separatorChar + "scripts" + File.separatorChar + "metastore" +
        File.separatorChar + "upgrade" + File.separatorChar + "derby" +
        File.separatorChar + preUpgradeScript;
    FileWriter fstream = new FileWriter(dummyPreScriptPath);
    BufferedWriter out = new BufferedWriter(fstream);
    out.write(sql + System.getProperty("line.separator") + ";");
    out.close();
    return preUpgradeScript;
  }

  /**
   * Insert the records in DB to simulate a hive table
   * @throws IOException
   */
  private void createTestHiveTableSchemas() throws IOException {
     String[] scripts = new String[] {
          "insert into CTLGS values(2, 'my_catalog', 'description', 'hdfs://myhost.com:8020/user/hive/warehouse/mydb')",
          "insert into DBS values(2, 'my db', 'hdfs://myhost.com:8021/user/hive/warehouse/mydb', 'mydb', 'public', 'role', 'my_catalog')",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (1,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/user/hive/warehouse/mydb',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
          "insert into SDS(SD_ID,CD_ID,INPUT_FORMAT,IS_COMPRESSED,LOCATION,NUM_BUCKETS,OUTPUT_FORMAT,SERDE_ID) values (2,null,'org.apache.hadoop.mapred.TextInputFormat','N','hdfs://myhost.com:8020/user/admin/2015_11_18',-1,'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',null)",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (2 ,1435255431,2,0 ,'hive',0,1,'mytal','MANAGED_TABLE',NULL,NULL,'n')",
          "insert into TBLS(TBL_ID,CREATE_TIME,DB_ID,LAST_ACCESS_TIME,OWNER,RETENTION,SD_ID,TBL_NAME,TBL_TYPE,VIEW_EXPANDED_TEXT,VIEW_ORIGINAL_TEXT,IS_REWRITE_ENABLED) values (3 ,1435255431,2,0 ,'hive',0,2,'aTable','MANAGED_TABLE',NULL,NULL,'n')",
          "insert into PARTITIONS(PART_ID,CREATE_TIME,LAST_ACCESS_TIME, PART_NAME,SD_ID,TBL_ID) values(1, 1441402388,0, 'd1=1/d2=1',2,2)"
        };
     File scriptFile = generateTestScript(scripts);
     schemaTool.runBeeLine(scriptFile.getPath());
  }

  /**
   * A mock Connection class that throws an exception out of getMetaData().
   */
  class BadMetaDataConnection extends DelegatingConnection {
    static final String FAILURE_TEXT = "fault injected";

    BadMetaDataConnection(Connection connection) {
      super(connection);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      throw new SQLException(FAILURE_TEXT);
    }
  }

  private void execute(HiveSchemaToolTask task, String taskArgs) throws HiveMetaException {
    try {
      StrTokenizer tokenizer = new StrTokenizer(argsBase + taskArgs, ' ', '\"');
      HiveSchemaToolCommandLine cl = new HiveSchemaToolCommandLine(tokenizer.getTokenArray());
      task.setCommandLineArguments(cl);
    } catch (Exception e) {
      throw new IllegalStateException("Could not parse comman line \n" + argsBase + taskArgs, e);
    }

    task.setHiveSchemaTool(schemaTool);
    task.execute();
  }
}

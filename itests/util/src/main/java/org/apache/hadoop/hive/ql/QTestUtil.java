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

package org.apache.hadoop.hive.ql;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.common.io.DigestPrintStream;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.common.io.SortAndDigestPrintStream;
import org.apache.hadoop.hive.common.io.SortPrintStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.QTestMiniClusters.CoreClusterType;
import org.apache.hadoop.hive.ql.QTestMiniClusters.FsType;
import org.apache.hadoop.hive.ql.QTestMiniClusters.MiniClusterType;
import org.apache.hadoop.hive.ql.QTestMiniClusters.QTestSetup;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.dataset.QTestDatasetHandler;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.NotificationEventPoll;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.StreamPrinter;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * QTestUtil.
 */
public class QTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger("QTestUtil");
  private static final String UTF_8 = "UTF-8";

  public static final String QTEST_LEAVE_FILES = "QTEST_LEAVE_FILES";
  private final static String DEFAULT_INIT_SCRIPT = "q_test_init.sql";
  private final static String DEFAULT_CLEANUP_SCRIPT = "q_test_cleanup.sql";
  private final String[] testOnlyCommands = new String[]{ "crypto", "erasure" };
  public static String DEBUG_HINT =
      "\nSee ./ql/target/tmp/log/hive.log or ./itests/qtest/target/tmp/log/hive.log, "
          + "or check ./ql/target/surefire-reports or ./itests/qtest/target/surefire-reports/ for specific test cases logs.";

  private String testWarehouse;
  @Deprecated private final String testFiles;
  private final String outDir;
  protected final String logDir;
  private final TreeMap<String, String> qMap;
  private final Set<String> qSortSet;
  private final Set<String> qSortQuerySet;
  private final Set<String> qHashQuerySet;
  private final Set<String> qSortNHashQuerySet;
  private final Set<String> qNoSessionReuseQuerySet;
  private static final String SORT_SUFFIX = ".sorted";
  private static Set<String> srcTables;
  private final Set<String> srcUDFs;
  private final MiniClusterType clusterType;
  private final FsType fsType;
  private ParseDriver pd;
  protected Hive db;
  private QueryState queryState;
  protected HiveConf conf;
  protected HiveConf savedConf;
  private IDriver drv;
  private BaseSemanticAnalyzer sem;
  private CliDriver cliDriver;
  private final QTestSetup setup;
  private boolean isSessionStateStarted = false;
  private QTestMiniClusters miniClusters;
  private QOutProcessor qOutProcessor;
  private QTestDatasetHandler datasetHandler;
  private final String initScript;
  private final String cleanupScript;

  public static Set<String> getSrcTables() {
    if (srcTables == null) {
      initSrcTables();
    }
    return srcTables;
  }

  public static void addSrcTable(String table) {
    getSrcTables().add(table);
    storeSrcTables();
  }

  public static Set<String> initSrcTables() {
    if (srcTables == null) {
      initSrcTablesFromSystemProperty();
      storeSrcTables();
    }

    return srcTables;
  }

  private static void storeSrcTables() {
    QTestSystemProperties.setSrcTables(srcTables);
  }

  private static void initSrcTablesFromSystemProperty() {
    srcTables = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    for (String srcTable : QTestSystemProperties.getSrcTables()) {
      srcTable = srcTable.trim();
      if (!srcTable.isEmpty()) {
        srcTables.add(srcTable);
      }
    }
  }

  private CliDriver getCliDriver() {
    if (cliDriver == null) {
      throw new RuntimeException("no clidriver");
    }
    return cliDriver;
  }

  /**
   * Returns the default UDF names which should not be removed when resetting the test database
   *
   * @return The list of the UDF names not to remove
   */
  private Set<String> getSrcUDFs() {
    HashSet<String> srcUDFs = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    String defaultTestSrcUDFs = "qtest_get_java_boolean";
    for (String srcUDF : QTestSystemProperties.getSourceUdfs(defaultTestSrcUDFs)) {
      srcUDF = srcUDF.trim();
      if (!srcUDF.isEmpty()) {
        srcUDFs.add(srcUDF);
      }
    }
    if (srcUDFs.isEmpty()) {
      throw new RuntimeException("Source UDFs cannot be empty");
    }
    return srcUDFs;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void initConf() throws Exception {
    if (QTestSystemProperties.isVectorizationEnabled()) {
      conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    }

    // Plug verifying metastore in for testing DirectSQL.
    conf.setVar(ConfVars.METASTORE_RAW_STORE_IMPL, "org.apache.hadoop.hive.metastore.VerifyingObjectStore");

    miniClusters.initConf(conf);
  }

  public QTestUtil(QTestArguments testArgs) throws Exception {
    LOG.info("Setting up QTestUtil with outDir={}, logDir={}, clusterType={}, confDir={},"
            + " initScript={}, cleanupScript={}, withLlapIo={}, fsType={}",
        testArgs.getOutDir(),
        testArgs.getLogDir(),
        testArgs.getClusterType(),
        testArgs.getConfDir(),
        testArgs.getInitScript(),
        testArgs.getCleanupScript(),
        testArgs.isWithLlapIo(),
        testArgs.getFsType());

    Preconditions.checkNotNull(testArgs.getClusterType(), "ClusterType cannot be null");

    this.fsType = testArgs.getFsType();
    this.outDir = testArgs.getOutDir();
    this.logDir = testArgs.getLogDir();
    this.srcUDFs = getSrcUDFs();
    this.qOutProcessor = new QOutProcessor(fsType);

    // HIVE-14443 move this fall-back logic to CliConfigs
    if (testArgs.getConfDir() != null && !testArgs.getConfDir().isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"
          + new File(testArgs.getConfDir()).toURI().getPath()
          + "/hive-site.xml"));
      MetastoreConf.setHiveSiteLocation(HiveConf.getHiveSiteLocation());
      System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    }

    queryState = new QueryState.Builder().withHiveConf(new HiveConf(IDriver.class)).build();
    conf = queryState.getConf();
    qMap = new TreeMap<String, String>();
    qSortSet = new HashSet<String>();
    qSortQuerySet = new HashSet<String>();
    qHashQuerySet = new HashSet<String>();
    qSortNHashQuerySet = new HashSet<String>();
    qNoSessionReuseQuerySet = new HashSet<String>();
    this.clusterType = testArgs.getClusterType();

    this.miniClusters = new QTestMiniClusters(fsType, conf);

    setup = testArgs.getQTestSetup();
    setup.preTest(conf);

    this.miniClusters.setup(setup, clusterType, conf, testArgs.getConfDir(), getScriptsDir(), logDir);

    initConf();

    if (testArgs.isWithLlapIo() && (clusterType == MiniClusterType.none)) {
      LOG.info("initializing llap IO");
      LlapProxy.initializeLlapIo(conf);
    }

    testFiles = datasetHandler.getDataDir(conf);
    conf.set("test.data.dir", datasetHandler.getDataDir(conf));

    datasetHandler = new QTestDatasetHandler(this, conf);

    String scriptsDir = getScriptsDir();

    this.initScript = scriptsDir + File.separator + testArgs.getInitScript();
    this.cleanupScript = scriptsDir + File.separator + testArgs.getCleanupScript();

    init();
    savedConf = new HiveConf(conf);
  }

  private String getScriptsDir() {
    // Use the current directory if it is not specified
    String scriptsDir = conf.get("test.data.scripts");
    if (scriptsDir == null) {
      scriptsDir = new File(".").getAbsolutePath() + "/data/scripts";
    }
    return scriptsDir;
  }

  public void shutdown() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) == null) {
      cleanUp();
    }

    if (clusterType.getCoreClusterType() == CoreClusterType.TEZ && SessionState.get().getTezSession() != null) {
      SessionState.get().getTezSession().destroy();
    }
    
    miniClusters.shutDown();

    Hive.closeCurrent();
  }

  public String readEntireFileIntoString(File queryFile) throws IOException {
    InputStreamReader
        isr =
        new InputStreamReader(new BufferedInputStream(new FileInputStream(queryFile)), QTestUtil.UTF_8);
    StringWriter sw = new StringWriter();
    try {
      IOUtils.copy(isr, sw);
    } finally {
      if (isr != null) {
        isr.close();
      }
    }
    return sw.toString();
  }

  public void addFile(String queryFile) throws IOException {
    addFile(new File(queryFile), false);
  }

  public void addFile(File qf, boolean partial) throws IOException {
    String query = readEntireFileIntoString(qf);
    qMap.put(qf.getName(), query);
    if (partial) {
      return;
    }

    if (matches(SORT_BEFORE_DIFF, query)) {
      qSortSet.add(qf.getName());
    } else if (matches(SORT_QUERY_RESULTS, query)) {
      qSortQuerySet.add(qf.getName());
    } else if (matches(HASH_QUERY_RESULTS, query)) {
      qHashQuerySet.add(qf.getName());
    } else if (matches(SORT_AND_HASH_QUERY_RESULTS, query)) {
      qSortNHashQuerySet.add(qf.getName());
    }
    if (matches(NO_SESSION_REUSE, query)) {
      qNoSessionReuseQuerySet.add(qf.getName());
    }

    qOutProcessor.initMasks(qf, query);
  }

  private static final Pattern SORT_BEFORE_DIFF = Pattern.compile("-- SORT_BEFORE_DIFF");
  private static final Pattern SORT_QUERY_RESULTS = Pattern.compile("-- SORT_QUERY_RESULTS");
  private static final Pattern HASH_QUERY_RESULTS = Pattern.compile("-- HASH_QUERY_RESULTS");
  private static final Pattern SORT_AND_HASH_QUERY_RESULTS = Pattern.compile("-- SORT_AND_HASH_QUERY_RESULTS");
  private static final Pattern NO_SESSION_REUSE = Pattern.compile("-- NO_SESSION_REUSE");

  private boolean matches(Pattern pattern, String query) {
    Matcher matcher = pattern.matcher(query);
    if (matcher.find()) {
      return true;
    }
    return false;
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearPostTestEffects() throws Exception {
    setup.postTest(conf);
  }

  public void clearKeysCreatedInTests() {
    if (miniClusters.getHdfsEncryptionShim() == null) {
      return;
    }
    try {
      for (String keyAlias : miniClusters.getHdfsEncryptionShim().getKeys()) {
        miniClusters.getHdfsEncryptionShim().deleteKey(keyAlias);
      }
    } catch (IOException e) {
      LOG.error("Fail to clean the keys created in test due to the error", e);
    }
  }

  public void clearUDFsCreatedDuringTests() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    // Delete functions created by the tests
    // It is enough to remove functions from the default database, other databases are dropped
    for (String udfName : db.getFunctions(DEFAULT_DATABASE_NAME, ".*")) {
      if (!srcUDFs.contains(udfName)) {
        db.dropFunction(DEFAULT_DATABASE_NAME, udfName);
      }
    }
  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearTablesCreatedDuringTests() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }

    conf.set("hive.metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
    db = Hive.get(conf);

    // First delete any MVs to avoid race conditions
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      for (String tblName : db.getAllTables()) {
        Table tblObj = null;
        try {
          tblObj = db.getTable(tblName);
        } catch (InvalidTableException e) {
          LOG.warn("Trying to drop table " + e.getTableName() + ". But it does not exist.");
          continue;
        }
        // only remove MVs first
        if (!tblObj.isMaterializedView()) {
          continue;
        }
        db.dropTable(dbName, tblName, true, true, fsType == FsType.encrypted_hdfs);
      }
    }

    // Delete any tables other than the source tables
    // and any databases other than the default database.
    for (String dbName : db.getAllDatabases()) {
      SessionState.get().setCurrentDatabase(dbName);
      for (String tblName : db.getAllTables()) {
        if (!DEFAULT_DATABASE_NAME.equals(dbName) || !getSrcTables().contains(tblName)) {
          Table tblObj = null;
          try {
            tblObj = db.getTable(tblName);
          } catch (InvalidTableException e) {
            LOG.warn("Trying to drop table " + e.getTableName() + ". But it does not exist.");
            continue;
          }
          db.dropTable(dbName, tblName, true, true, fsNeedsPurge(fsType));
        }
      }
      if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
        // Drop cascade, functions dropped by cascade
        db.dropDatabase(dbName, true, true, true);
      }
    }

    // delete remaining directories for external tables (can affect stats for following tests)
    try {
      Path p = new Path(testWarehouse);
      FileSystem fileSystem = p.getFileSystem(conf);
      if (fileSystem.exists(p)) {
        for (FileStatus status : fileSystem.listStatus(p)) {
          if (status.isDirectory() && !getSrcTables().contains(status.getPath().getName())) {
            fileSystem.delete(status.getPath(), true);
          }
        }
      }
    } catch (IllegalArgumentException e) {
      // ignore.. provides invalid url sometimes intentionally
    }
    SessionState.get().setCurrentDatabase(DEFAULT_DATABASE_NAME);

    List<String> roleNames = db.getAllRoleNames();
    for (String roleName : roleNames) {
      if (!"PUBLIC".equalsIgnoreCase(roleName) && !"ADMIN".equalsIgnoreCase(roleName)) {
        db.dropRole(roleName);
      }
    }
  }

  public void newSession() throws Exception {
    newSession(true);
  }

  public void newSession(boolean canReuseSession) throws Exception {
    // allocate and initialize a new conf since a test can
    // modify conf by using 'set' commands
    conf = new HiveConf(savedConf);
    initConf();
    initConfFromSetup();

    // renew the metastore since the cluster type is unencrypted
    db = Hive.get(conf); // propagate new conf to meta store

    HiveConf.setVar(conf,
        HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.DummyAuthenticator");
    CliSessionState ss = new CliSessionState(conf);
    ss.in = System.in;

    SessionState oldSs = SessionState.get();
    restartSessions(canReuseSession, ss, oldSs);
    closeSession(oldSs);

    SessionState.start(ss);

    cliDriver = new CliDriver();

    File outf = new File(logDir, "initialize.log");
    setSessionOutputs("that_shouldnt_happen_there", ss, outf);

  }

  /**
   * Clear out any side effects of running tests
   */
  public void clearTestSideEffects() throws Exception {
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    // the test might have configured security/etc; open a new session to get rid of that
    newSession();

    // Remove any cached results from the previous test.
    Utilities.clearWorkMap(conf);
    NotificationEventPoll.shutdown();
    QueryResultsCache.cleanupInstance();
    clearTablesCreatedDuringTests();
    clearUDFsCreatedDuringTests();
    clearKeysCreatedInTests();
    StatsSources.clearGlobalStats();
    TxnDbUtil.cleanDb(conf);
    TxnDbUtil.prepDb(conf);
  }

  protected void initConfFromSetup() throws Exception {
    setup.preTest(conf);
  }

  public void cleanUp() throws Exception {
    cleanUp(null);
  }

  public void cleanUp(String fileName) throws Exception {
    boolean canReuseSession = (fileName == null) || !qNoSessionReuseQuerySet.contains(fileName);
    if (!isSessionStateStarted) {
      startSessionState(canReuseSession);
    }
    if (System.getenv(QTEST_LEAVE_FILES) != null) {
      return;
    }
    conf.setBoolean("hive.test.shutdown.phase", true);

    clearTablesCreatedDuringTests();
    clearUDFsCreatedDuringTests();
    clearKeysCreatedInTests();

    cleanupFromFile();

    // delete any contents in the warehouse dir
    Path p = new Path(testWarehouse);
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus[] ls = fs.listStatus(p);
      for (int i = 0; (ls != null) && (i < ls.length); i++) {
        fs.delete(ls[i].getPath(), true);
      }
    } catch (FileNotFoundException e) {
      // Best effort
    }

    // TODO: Clean up all the other paths that are created.

    FunctionRegistry.unregisterTemporaryUDF("test_udaf");
    FunctionRegistry.unregisterTemporaryUDF("test_error");
  }

  private void cleanupFromFile() throws IOException {
    File cleanupFile = new File(cleanupScript);
    if (cleanupFile.isFile()) {
      String cleanupCommands = readEntireFileIntoString(cleanupFile);
      LOG.info("Cleanup (" + cleanupScript + "):\n" + cleanupCommands);

      int result = getCliDriver().processLine(cleanupCommands);
      if (result != 0) {
        LOG.error("Failed during cleanup processLine with code={}. Ignoring", result);
        // TODO Convert this to an Assert.fail once HIVE-14682 is fixed
      }
    } else {
      LOG.info("No cleanup script detected. Skipping.");
    }
  }

  public void createSources() throws Exception {
    createSources(null);
  }

  public void createSources(String fileName) throws Exception {
    boolean canReuseSession = (fileName == null) || !qNoSessionReuseQuerySet.contains(fileName);
    if (!isSessionStateStarted) {
      startSessionState(canReuseSession);
    }

    getCliDriver().processLine("set test.data.dir=" + testFiles + ";");

    conf.setBoolean("hive.test.init.phase", true);

    initFromScript();

    conf.setBoolean("hive.test.init.phase", false);
  }

  private void initFromScript() throws IOException {
    File scriptFile = new File(this.initScript);
    if (!scriptFile.isFile()) {
      LOG.info("No init script detected. Skipping");
      return;
    }

    String initCommands = readEntireFileIntoString(scriptFile);
    LOG.info("Initial setup (" + initScript + "):\n" + initCommands);

    int result = cliDriver.processLine(initCommands);
    LOG.info("Result from cliDrriver.processLine in createSources=" + result);
    if (result != 0) {
      Assert.fail("Failed during createSources processLine with code=" + result);
    }
  }

  public void init() throws Exception {
    miniClusters.createRemoteDirs(conf);

    testWarehouse = conf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");
    SessionState.start(conf);
    conf.set("hive.execution.engine", execEngine);
    db = Hive.get(conf);
    // Create views registry
    String registryImpl = db.getConf().get("hive.server2.materializedviews.registry.impl");
    db.getConf().set("hive.server2.materializedviews.registry.impl", "DUMMY");
    HiveMaterializedViewsRegistry.get().init(db);
    db.getConf().set("hive.server2.materializedviews.registry.impl", registryImpl);
    drv = DriverFactory.newDriver(conf);
    pd = new ParseDriver();
    sem = new SemanticAnalyzer(queryState);
  }

  public void init(String fileName) throws Exception {
    cleanUp(fileName);
    createSources(fileName);
    cliDriver.processCmd("set hive.cli.print.header=true;");
  }

  public String cliInit(File file) throws Exception {
    String fileName = file.getName();

    datasetHandler.initDataSetForTest(file, getCliDriver());

    if (qNoSessionReuseQuerySet.contains(fileName)) {
      newSession(false);
    }

    CliSessionState ss = (CliSessionState) SessionState.get();

    String outFileExtension = getOutFileExtension(fileName);
    String stdoutName = null;

    if (outDir != null) {
      // TODO: why is this needed?
      File qf = new File(outDir, fileName);
      stdoutName = qf.getName().concat(outFileExtension);
    } else {
      stdoutName = fileName + outFileExtension;
    }
    File outf = new File(logDir, stdoutName);
    setSessionOutputs(fileName, ss, outf);

    if (fileName.equals("init_file.q")) {
      ss.initFiles.add(AbstractCliConfig.HIVE_ROOT + "/data/scripts/test_init_file.sql");
    }
    cliDriver.processInitFiles(ss);

    return outf.getAbsolutePath();
  }

  private void setSessionOutputs(String fileName, CliSessionState ss, File outf) throws Exception {
    OutputStream fo = new BufferedOutputStream(new FileOutputStream(outf));
    if (ss.out != null) {
      ss.out.flush();
    }
    if (ss.err != null) {
      ss.out.flush();
    }
    if (qSortQuerySet.contains(fileName)) {
      ss.out = new SortPrintStream(fo, "UTF-8");
    } else if (qHashQuerySet.contains(fileName)) {
      ss.out = new DigestPrintStream(fo, "UTF-8");
    } else if (qSortNHashQuerySet.contains(fileName)) {
      ss.out = new SortAndDigestPrintStream(fo, "UTF-8");
    } else {
      ss.out = new SessionStream(fo, true, "UTF-8");
    }
    ss.err = new CachingPrintStream(fo, true, "UTF-8");
    ss.setIsSilent(true);
  }

  private void restartSessions(boolean canReuseSession, CliSessionState ss, SessionState oldSs) throws IOException {
    if (oldSs != null && canReuseSession && clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      // Copy the tezSessionState from the old CliSessionState.
      TezSessionState tezSessionState = oldSs.getTezSession();
      oldSs.setTezSession(null);
      ss.setTezSession(tezSessionState);
      oldSs.close();
    }

    if (oldSs != null && clusterType.getCoreClusterType() == CoreClusterType.SPARK) {
      miniClusters.setSparkSession(oldSs.getSparkSession());
      ss.setSparkSession(miniClusters.getSparkSession());
      oldSs.setSparkSession(null);
      oldSs.close();
    }
  }

  private CliSessionState startSessionState(boolean canReuseSession) throws IOException {

    HiveConf.setVar(conf,
        HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.DummyAuthenticator");

    String execEngine = conf.get("hive.execution.engine");
    conf.set("hive.execution.engine", "mr");

    CliSessionState ss = new CliSessionState(conf);
    ss.in = System.in;
    ss.out = new SessionStream(System.out);
    ss.err = new SessionStream(System.out);

    SessionState oldSs = SessionState.get();

    restartSessions(canReuseSession, ss, oldSs);

    closeSession(oldSs);
    SessionState.start(ss);

    isSessionStateStarted = true;

    conf.set("hive.execution.engine", execEngine);
    return ss;
  }

  private void closeSession(SessionState oldSs) throws IOException {
    if (oldSs != null && oldSs.out != null && oldSs.out != System.out) {
      oldSs.out.close();
    }
    if (oldSs != null) {
      oldSs.close();
    }
  }

  public int executeAdhocCommand(String q) {
    if (!q.contains(";")) {
      return -1;
    }

    String q1 = q.split(";")[0] + ";";

    LOG.debug("Executing " + q1);
    return cliDriver.processLine(q1);
  }

  public int execute(String tname) {
    return drv.run(qMap.get(tname)).getResponseCode();
  }

  public int executeClient(String tname1, String tname2) {
    String commands = getCommand(tname1) +  System.getProperty("line.separator") + getCommand(tname2);
    return executeClientInternal(commands);
  }

  public int executeClient(String fileName) {
    return executeClientInternal(getCommand(fileName));
  }

  private int executeClientInternal(String commands) {
    List<String> cmds = CliDriver.splitSemiColon(commands);
    int rc = 0;

    String command = "";
    for (String oneCmd : cmds) {
      if (StringUtils.endsWith(oneCmd, "\\")) {
        command += StringUtils.chop(oneCmd) + "\\;";
        continue;
      } else {
        if (isHiveCommand(oneCmd)) {
          command = oneCmd;
        } else {
          command += oneCmd;
        }
      }
      if (StringUtils.isBlank(command)) {
        continue;
      }

      if (isCommandUsedForTesting(command)) {
        rc = executeTestCommand(command);
      } else {
        rc = cliDriver.processLine(command);
      }

      if (rc != 0 && !ignoreErrors()) {
        break;
      }
      command = "";
    }
    if (rc == 0 && SessionState.get() != null) {
      SessionState.get().setLastCommand(null);  // reset
    }
    return rc;
  }

  /**
   * This allows a .q file to continue executing after a statement runs into an error which is convenient
   * if you want to use another hive cmd after the failure to sanity check the state of the system.
   */
  private boolean ignoreErrors() {
    return conf.getBoolVar(HiveConf.ConfVars.CLIIGNOREERRORS);
  }

  private boolean isHiveCommand(String command) {
    String[] cmd = command.trim().split("\\s+");
    if (HiveCommand.find(cmd) != null) {
      return true;
    } else if (HiveCommand.find(cmd, HiveCommand.ONLY_FOR_TESTING) != null) {
      return true;
    } else {
      return false;
    }
  }

  private int executeTestCommand(final String command) {
    String commandName = command.trim().split("\\s+")[0];
    String commandArgs = command.trim().substring(commandName.length());

    if (commandArgs.endsWith(";")) {
      commandArgs = StringUtils.chop(commandArgs);
    }

    //replace ${hiveconf:hive.metastore.warehouse.dir} with actual dir if existed.
    //we only want the absolute path, so remove the header, such as hdfs://localhost:57145
    String
        wareHouseDir =
        SessionState.get().getConf().getVar(ConfVars.METASTOREWAREHOUSE).replaceAll("^[a-zA-Z]+://.*?:\\d+", "");
    commandArgs = commandArgs.replaceAll("\\$\\{hiveconf:hive\\.metastore\\.warehouse\\.dir\\}", wareHouseDir);

    if (SessionState.get() != null) {
      SessionState.get().setLastCommand(commandName + " " + commandArgs.trim());
    }

    enableTestOnlyCmd(SessionState.get().getConf());

    try {
      CommandProcessor proc = getTestCommand(commandName);
      if (proc != null) {
        CommandProcessorResponse response = proc.run(commandArgs.trim());

        int rc = response.getResponseCode();
        if (rc != 0) {
          SessionState.getConsole()
              .printError(response.toString(),
                  response.getException() != null ? Throwables.getStackTraceAsString(response.getException()) : "");
        }

        return rc;
      } else {
        throw new RuntimeException("Could not get CommandProcessor for command: " + commandName);
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not execute test command", e);
    }
  }

  private CommandProcessor getTestCommand(final String commandName) throws SQLException {
    HiveCommand testCommand = HiveCommand.find(new String[]{ commandName }, HiveCommand.ONLY_FOR_TESTING);

    if (testCommand == null) {
      return null;
    }

    return CommandProcessorFactory.getForHiveCommandInternal(new String[]{ commandName },
        SessionState.get().getConf(),
        testCommand.isOnlyForTesting());
  }

  private void enableTestOnlyCmd(HiveConf conf) {
    StringBuilder securityCMDs = new StringBuilder(conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST));
    for (String c : testOnlyCommands) {
      securityCMDs.append(",");
      securityCMDs.append(c);
    }
    conf.set(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.toString(), securityCMDs.toString());
  }

  private boolean isCommandUsedForTesting(final String command) {
    String commandName = command.trim().split("\\s+")[0];
    HiveCommand testCommand = HiveCommand.find(new String[]{ commandName }, HiveCommand.ONLY_FOR_TESTING);
    return testCommand != null;
  }

  private String getCommand(String fileName) {
    String commands = qMap.get(fileName);
    StringBuilder newCommands = new StringBuilder(commands.length());
    int lastMatchEnd = 0;
    Matcher commentMatcher = Pattern.compile("^--.*$", Pattern.MULTILINE).matcher(commands);
    // remove the comments
    while (commentMatcher.find()) {
      newCommands.append(commands.substring(lastMatchEnd, commentMatcher.start()));
      lastMatchEnd = commentMatcher.end();
    }
    newCommands.append(commands.substring(lastMatchEnd, commands.length()));
    commands = newCommands.toString();
    return commands;
  }

  private String getOutFileExtension(String fname) {
    return ".out";
  }

  public QTestProcessExecResult checkNegativeResults(String tname, Exception e) throws Exception {

    String outFileExtension = getOutFileExtension(tname);

    File qf = new File(outDir, tname);
    String expf = outPath(outDir.toString(), tname.concat(outFileExtension));

    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(outFileExtension));

    FileWriter outfd = new FileWriter(outf);
    if (e instanceof ParseException) {
      outfd.write("Parse Error: ");
    } else if (e instanceof SemanticException) {
      outfd.write("Semantic Exception: \n");
    } else {
      outfd.close();
      throw e;
    }

    outfd.write(e.getMessage());
    outfd.close();

    QTestProcessExecResult result = executeDiffCommand(outf.getPath(), expf, false, qSortSet.contains(qf.getName()));
    if (QTestSystemProperties.shouldOverwriteResults()) {
      overwriteResults(outf.getPath(), expf);
      return QTestProcessExecResult.createWithoutOutput(0);
    }

    return result;
  }

  public QTestProcessExecResult checkNegativeResults(String tname, Error e) throws Exception {

    String outFileExtension = getOutFileExtension(tname);

    File qf = new File(outDir, tname);
    String expf = outPath(outDir.toString(), tname.concat(outFileExtension));

    File outf = null;
    outf = new File(logDir);
    outf = new File(outf, qf.getName().concat(outFileExtension));

    FileWriter outfd = new FileWriter(outf, true);

    outfd.write("FAILED: "
        + e.getClass().getSimpleName()
        + " "
        + e.getClass().getName()
        + ": "
        + e.getMessage()
        + "\n");
    outfd.close();

    QTestProcessExecResult result = executeDiffCommand(outf.getPath(), expf, false, qSortSet.contains(qf.getName()));
    if (QTestSystemProperties.shouldOverwriteResults()) {
      overwriteResults(outf.getPath(), expf);
      return QTestProcessExecResult.createWithoutOutput(0);
    }

    return result;
  }

  /**
   * Given the current configurations (e.g., hadoop version and execution mode), return
   * the correct file name to compare with the current test run output.
   *
   * @param outDir   The directory where the reference log files are stored.
   * @param testName The test file name (terminated by ".out").
   * @return The file name appended with the configuration values if it exists.
   */
  public String outPath(String outDir, String testName) {
    String ret = (new File(outDir, testName)).getPath();
    // List of configurations. Currently the list consists of hadoop version and execution mode only
    List<String> configs = new ArrayList<String>();
    configs.add(this.clusterType.toString());

    Deque<String> stack = new LinkedList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append(testName);
    stack.push(sb.toString());

    // example file names are input1.q.out_mr_0.17 or input2.q.out_0.17
    for (String s : configs) {
      sb.append('_');
      sb.append(s);
      stack.push(sb.toString());
    }
    while (stack.size() > 0) {
      String fileName = stack.pop();
      File f = new File(outDir, fileName);
      if (f.exists()) {
        ret = f.getPath();
        break;
      }
    }
    return ret;
  }

  public QTestProcessExecResult checkCliDriverResults(String tname) throws Exception {
    assert (qMap.containsKey(tname));

    String outFileExtension = getOutFileExtension(tname);
    String outFileName = outPath(outDir, tname + outFileExtension);

    File f = new File(logDir, tname + outFileExtension);

    qOutProcessor.maskPatterns(f.getPath(), tname);
    QTestProcessExecResult exitVal = executeDiffCommand(f.getPath(), outFileName, false, qSortSet.contains(tname));

    if (QTestSystemProperties.shouldOverwriteResults()) {
      overwriteResults(f.getPath(), outFileName);
      return QTestProcessExecResult.createWithoutOutput(0);
    }

    return exitVal;
  }

  public QTestProcessExecResult checkCompareCliDriverResults(String tname, List<String> outputs) throws Exception {
    assert outputs.size() > 1;
    qOutProcessor.maskPatterns(outputs.get(0), tname);
    for (int i = 1; i < outputs.size(); ++i) {
      qOutProcessor.maskPatterns(outputs.get(i), tname);
      QTestProcessExecResult
          result =
          executeDiffCommand(outputs.get(i - 1), outputs.get(i), false, qSortSet.contains(tname));
      if (result.getReturnCode() != 0) {
        System.out.println("Files don't match: " + outputs.get(i - 1) + " and " + outputs.get(i));
        return result;
      }
    }
    return QTestProcessExecResult.createWithoutOutput(0);
  }

  private static void overwriteResults(String inFileName, String outFileName) throws Exception {
    // This method can be replaced with Files.copy(source, target, REPLACE_EXISTING)
    // once Hive uses JAVA 7.
    System.out.println("Overwriting results " + inFileName + " to " + outFileName);
    int result = executeCmd(new String[]{
        "cp", getQuotedString(inFileName), getQuotedString(outFileName) }).getReturnCode();
    if (result != 0) {
      throw new IllegalStateException("Unexpected error while overwriting " + inFileName + " with " + outFileName);
    }
  }

  private static QTestProcessExecResult executeDiffCommand(String inFileName,
      String outFileName,
      boolean ignoreWhiteSpace,
      boolean sortResults) throws Exception {

    QTestProcessExecResult result;

    if (sortResults) {
      // sort will try to open the output file in write mode on windows. We need to
      // close it first.
      SessionState ss = SessionState.get();
      if (ss != null && ss.out != null && ss.out != System.out) {
        ss.out.close();
      }

      String inSorted = inFileName + SORT_SUFFIX;
      String outSorted = outFileName + SORT_SUFFIX;

      sortFiles(inFileName, inSorted);
      sortFiles(outFileName, outSorted);

      inFileName = inSorted;
      outFileName = outSorted;
    }

    ArrayList<String> diffCommandArgs = new ArrayList<String>();
    diffCommandArgs.add("diff");

    // Text file comparison
    diffCommandArgs.add("-a");

    // Ignore changes in the amount of white space
    if (ignoreWhiteSpace) {
      diffCommandArgs.add("-b");
    }

    // Add files to compare to the arguments list
    diffCommandArgs.add(getQuotedString(inFileName));
    diffCommandArgs.add(getQuotedString(outFileName));

    result = executeCmd(diffCommandArgs);

    if (sortResults) {
      new File(inFileName).delete();
      new File(outFileName).delete();
    }

    return result;
  }

  private static void sortFiles(String in, String out) throws Exception {
    int result = executeCmd(new String[]{
        "sort", getQuotedString(in), }, out, null).getReturnCode();
    if (result != 0) {
      throw new IllegalStateException("Unexpected error while sorting " + in);
    }
  }

  private static QTestProcessExecResult executeCmd(Collection<String> args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static QTestProcessExecResult executeCmd(String[] args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static QTestProcessExecResult executeCmd(Collection<String> args, String outFile, String errFile)
      throws Exception {
    String[] cmdArray = args.toArray(new String[args.size()]);
    return executeCmd(cmdArray, outFile, errFile);
  }

  private static QTestProcessExecResult executeCmd(String[] args, String outFile, String errFile) throws Exception {
    System.out.println("Running: " + org.apache.commons.lang.StringUtils.join(args, ' '));

    PrintStream
        out =
        outFile == null ?
            SessionState.getConsole().getChildOutStream() :
            new PrintStream(new FileOutputStream(outFile), true, "UTF-8");
    PrintStream
        err =
        errFile == null ?
            SessionState.getConsole().getChildErrStream() :
            new PrintStream(new FileOutputStream(errFile), true, "UTF-8");

    Process executor = Runtime.getRuntime().exec(args);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintStream str = new PrintStream(bos, true, "UTF-8");

    StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, err);
    StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, out, str);

    outPrinter.start();
    errPrinter.start();

    int result = executor.waitFor();

    outPrinter.join();
    errPrinter.join();

    if (outFile != null) {
      out.close();
    }

    if (errFile != null) {
      err.close();
    }

    return QTestProcessExecResult.
        create(result, new String(bos.toByteArray(), StandardCharsets.UTF_8));
  }

  private static String getQuotedString(String str) {
    return str;
  }

  public ASTNode parseQuery(String tname) throws Exception {
    return pd.parse(qMap.get(tname));
  }

  public List<Task<?>> analyzeAST(ASTNode ast) throws Exception {

    // Do semantic analysis and plan generation
    Context ctx = new Context(conf);
    while ((ast.getToken() == null) && (ast.getChildCount() > 0)) {
      ast = (ASTNode) ast.getChild(0);
    }
    sem.getOutputs().clear();
    sem.getInputs().clear();
    sem.analyze(ast, ctx);
    ctx.clear();
    return sem.getRootTasks();
  }


  /**
   * QTRunner: Runnable class for running a single query file.
   **/
  public static class QTRunner implements Runnable {
    private final QTestUtil qt;
    private final File file;

    public QTRunner(QTestUtil qt, File file) {
      this.qt = qt;
      this.file = file;
    }

    @Override public void run() {
      try {
        qt.startSessionState(false);
        // assumption is that environment has already been cleaned once globally
        // hence each thread does not call cleanUp() and createSources() again
        qt.cliInit(file);
        qt.executeClient(file.getName());
      } catch (Throwable e) {
        System.err.println("Query file " + file.getName() + " failed with exception " + e.getMessage());
        e.printStackTrace();
        outputTestFailureHelpMessage();
      }
    }
  }

  /**
   * Setup to execute a set of query files. Uses QTestUtil to do so.
   *
   * @param qfiles array of input query files containing arbitrary number of hive
   *               queries
   * @param resDir output directory
   * @param logDir log directory
   * @return one QTestUtil for each query file
   */
  public static QTestUtil[] queryListRunnerSetup(File[] qfiles,
      String resDir,
      String logDir,
      String initScript,
      String cleanupScript) throws Exception {
    QTestUtil[] qt = new QTestUtil[qfiles.length];
    for (int i = 0; i < qfiles.length; i++) {

      qt[i] =
          new QTestUtil(QTestArguments.QTestArgumentsBuilder.instance()
              .withOutDir(resDir)
              .withLogDir(logDir)
              .withClusterType(MiniClusterType.none)
              .withConfDir(null)
              .withInitScript(initScript == null ? DEFAULT_INIT_SCRIPT : initScript)
              .withCleanupScript(cleanupScript == null ? DEFAULT_CLEANUP_SCRIPT : cleanupScript)
              .withLlapIo(false)
              .build());

      qt[i].addFile(qfiles[i], false);
      qt[i].clearTestSideEffects();
    }

    return qt;
  }

  /**
   * Executes a set of query files in sequence.
   *
   * @param qfiles array of input query files containing arbitrary number of hive
   *               queries
   * @param qt     array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerSingleThreaded(File[] qfiles, QTestUtil[] qt) throws Exception {
    boolean failed = false;
    qt[0].cleanUp();
    qt[0].createSources();
    for (int i = 0; i < qfiles.length && !failed; i++) {
      qt[i].clearTestSideEffects();
      qt[i].cliInit(qfiles[i]);
      qt[i].executeClient(qfiles[i].getName());
      QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (result.getReturnCode() != 0) {
        failed = true;
        StringBuilder builder = new StringBuilder();
        builder.append("Test ")
            .append(qfiles[i].getName())
            .append(" results check failed with error code ")
            .append(result.getReturnCode());
        if (Strings.isNotEmpty(result.getCapturedOutput())) {
          builder.append(" and diff value ").append(result.getCapturedOutput());
        }
        System.err.println(builder.toString());
        outputTestFailureHelpMessage();
      }
      qt[i].clearPostTestEffects();
    }
    return (!failed);
  }

  /**
   * Executes a set of query files parallel.
   * <p>
   * Each query file is run in a separate thread. The caller has to arrange
   * that different query files do not collide (in terms of destination tables)
   *
   * @param qfiles array of input query files containing arbitrary number of hive
   *               queries
   * @param qt     array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerMultiThreaded(File[] qfiles, QTestUtil[] qt) throws Exception {
    boolean failed = false;

    // in multithreaded mode - do cleanup/initialization just once

    qt[0].cleanUp();
    qt[0].createSources();
    qt[0].clearTestSideEffects();

    QTRunner[] qtRunners = new QTRunner[qfiles.length];
    Thread[] qtThread = new Thread[qfiles.length];

    for (int i = 0; i < qfiles.length; i++) {
      qtRunners[i] = new QTRunner(qt[i], qfiles[i]);
      qtThread[i] = new Thread(qtRunners[i]);
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].start();
    }

    for (int i = 0; i < qfiles.length; i++) {
      qtThread[i].join();
      QTestProcessExecResult result = qt[i].checkCliDriverResults(qfiles[i].getName());
      if (result.getReturnCode() != 0) {
        failed = true;
        StringBuilder builder = new StringBuilder();
        builder.append("Test ")
            .append(qfiles[i].getName())
            .append(" results check failed with error code ")
            .append(result.getReturnCode());
        if (Strings.isNotEmpty(result.getCapturedOutput())) {
          builder.append(" and diff value ").append(result.getCapturedOutput());
        }
        System.err.println(builder.toString());
        outputTestFailureHelpMessage();
      }
    }
    return (!failed);
  }

  public static void outputTestFailureHelpMessage() {
    System.err.println(DEBUG_HINT);
    System.err.flush();
  }

  public void failed(int ecode, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
    String
        message =
        "Client execution failed with error code = "
            + ecode
            + (command != null ? " running \"" + command : "")
            + "\" fname="
            + fname
            + " "
            + (debugHint != null ? debugHint : "");
    LOG.error(message);
    Assert.fail(message);
  }

  // for negative tests, which is succeeded.. no need to print the query string
  public void failed(String fname, String debugHint) {
    Assert.fail("Client Execution was expected to fail, but succeeded with error code 0 for fname=" + fname + (debugHint
        != null ? (" " + debugHint) : ""));
  }

  public void failedDiff(int ecode, String fname, String debugHint) {
    String
        message =
        "Client Execution succeeded but contained differences "
            + "(error code = "
            + ecode
            + ") after executing "
            + fname
            + (debugHint != null ? (" " + debugHint) : "");
    LOG.error(message);
    Assert.fail(message);
  }

  public void failed(Exception e, String fname, String debugHint) {
    String command = SessionState.get() != null ? SessionState.get().getLastCommand() : null;
    System.err.println("Failed query: " + fname);
    System.err.flush();
    Assert.fail("Unexpected exception " + org.apache.hadoop.util.StringUtils.stringifyException(e) + "\n" + (command
        != null ? " running " + command : "") + (debugHint != null ? debugHint : ""));
  }

  public QOutProcessor getQOutProcessor() {
    return qOutProcessor;
  }

  public static void initEventNotificationPoll() throws Exception {
    NotificationEventPoll.initialize(SessionState.get().getConf());
  }

  /**
   * Should deleted test tables have their data purged.
   *
   * @return true if data should be purged
   */
  private static boolean fsNeedsPurge(FsType type) {
    if (type == FsType.encrypted_hdfs || type == FsType.erasure_coded_hdfs) {
      return true;
    }
    return false;
  }
}

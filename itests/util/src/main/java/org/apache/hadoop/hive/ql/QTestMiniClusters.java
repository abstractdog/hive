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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapItUtils;
import org.apache.hadoop.hive.llap.daemon.MiniLlapCluster;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.CuratorFrameworkSingleton;
import org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsErasureCodingShim;
import org.apache.hive.druid.MiniDruidCluster;
import org.apache.hive.kafka.SingleNodeKafkaCluster;
import org.apache.hive.kafka.Wikipedia;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * QTestMiniClusters: decouples cluster details from QTestUtil (kafka/druid/spark/llap/tez/mr, file system)
 */
public class QTestMiniClusters {
  private static final Logger LOG = LoggerFactory.getLogger("QTestMiniClusters");
  private static final SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

  // security property names
  private static final String SECURITY_KEY_PROVIDER_URI_NAME = "dfs.encryption.key.provider.uri";
  /**
   * The default Erasure Coding Policy to use in Erasure Coding tests.
   */
  public static final String DEFAULT_TEST_EC_POLICY = "RS-3-2-1024k";

  private QTestSetup setup;
  private HadoopShims shims;
  private SparkSession sparkSession = null;
  private FileSystem fs;
  private HadoopShims.MiniMrShim mr = null;
  private HadoopShims.MiniDFSShim dfs = null;
  private HadoopShims.HdfsEncryptionShim hes = null;
  private MiniLlapCluster llapCluster = null;
  private MiniDruidCluster druidCluster = null;
  private SingleNodeKafkaCluster kafkaCluster = null;

  public enum CoreClusterType {
    MR, TEZ, SPARK
  }

  public enum FsType {
    local, hdfs, encrypted_hdfs, erasure_coded_hdfs,
  }

  public enum MiniClusterType {

    mr(CoreClusterType.MR, FsType.hdfs), tez(CoreClusterType.TEZ, FsType.hdfs), tez_local(
        CoreClusterType.TEZ,
        FsType.local), spark(CoreClusterType.SPARK, FsType.local), miniSparkOnYarn(
            CoreClusterType.SPARK, FsType.hdfs), llap(CoreClusterType.TEZ, FsType.hdfs), llap_local(
                CoreClusterType.TEZ, FsType.local), none(CoreClusterType.MR,
                    FsType.local), druidLocal(CoreClusterType.TEZ, FsType.local), druid(
                        CoreClusterType.TEZ, FsType.hdfs), druidKafka(CoreClusterType.TEZ,
                            FsType.hdfs), kafka(CoreClusterType.TEZ, FsType.hdfs);

    private final CoreClusterType coreClusterType;
    private final FsType defaultFsType;

    MiniClusterType(CoreClusterType coreClusterType, FsType defaultFsType) {
      this.coreClusterType = coreClusterType;
      this.defaultFsType = defaultFsType;
    }

    public CoreClusterType getCoreClusterType() {
      return coreClusterType;
    }

    public FsType getDefaultFsType() {
      return defaultFsType;
    }

    public static MiniClusterType valueForString(String type) {
      // Replace this with valueOf.
      if (type.equals("miniMR")) {
        return mr;
      } else if (type.equals("tez")) {
        return tez;
      } else if (type.equals("tez_local")) {
        return tez_local;
      } else if (type.equals("spark")) {
        return spark;
      } else if (type.equals("miniSparkOnYarn")) {
        return miniSparkOnYarn;
      } else if (type.equals("llap")) {
        return llap;
      } else if (type.equals("llap_local")) {
        return llap_local;
      } else if (type.equals("druidLocal")) {
        return druidLocal;
      } else if (type.equals("druid")) {
        return druid;
      } else if (type.equals("druid-kafka")) {
        return druidKafka;
      } else if (type.equals("kafka")) {
        return kafka;
      } else {
        return none;
      }
    }
  }

  /**
   * QTestSetup defines test fixtures which are reused across testcases, and are needed before any
   * test can be run
   */
  public static class QTestSetup {
    private MiniZooKeeperCluster zooKeeperCluster = null;
    private int zkPort;
    private ZooKeeper zooKeeper;

    public QTestSetup() {
    }

    public void preTest(HiveConf conf) throws Exception {

      if (zooKeeperCluster == null) {
        // create temp dir
        File tmpDir = Files
            .createTempDirectory(Paths.get(QTestSystemProperties.getTempDir()), "tmp_").toFile();

        zooKeeperCluster = new MiniZooKeeperCluster();
        zkPort = zooKeeperCluster.startup(tmpDir);
      }

      if (zooKeeper != null) {
        zooKeeper.close();
      }

      int sessionTimeout = (int) conf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
          TimeUnit.MILLISECONDS);
      zooKeeper = new ZooKeeper("localhost:" + zkPort, sessionTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent arg0) {
        }
      });

      String zkServer = "localhost";
      conf.set("hive.zookeeper.quorum", zkServer);
      conf.set("hive.zookeeper.client.port", "" + zkPort);
    }

    public void postTest(HiveConf conf) throws Exception {
      if (zooKeeperCluster == null) {
        return;
      }

      if (zooKeeper != null) {
        zooKeeper.close();
      }

      ZooKeeperHiveLockManager.releaseAllLocks(conf);
    }

    public void tearDown() throws Exception {
      CuratorFrameworkSingleton.closeAndReleaseInstance();

      if (zooKeeperCluster != null) {
        zooKeeperCluster.shutdown();
        zooKeeperCluster = null;
      }
    }
  }

  public QTestMiniClusters(FsType fsType, HiveConf hiveConf) throws IOException {
    this.shims = ShimLoader.getHadoopShims();
    setupFileSystem(fsType, hiveConf);
  }

  public void setup(QTestSetup setup, MiniClusterType clusterType, HiveConf conf, String confDir,
      String scriptsDir, String logDir) throws IOException {
    this.setup = setup;

    String uriString = fs.getUri().toString();

    if (clusterType == MiniClusterType.druidKafka || clusterType == MiniClusterType.druidLocal
        || clusterType == MiniClusterType.druid) {
      final String tempDir = QTestSystemProperties.getTempDir();
      druidCluster = new MiniDruidCluster(
          clusterType == MiniClusterType.druid ? "mini-druid" : "mini-druid-kafka", logDir, tempDir,
          setup.zkPort, Utilities.jarFinderGetJar(MiniDruidCluster.class));
      final Path druidDeepStorage = fs.makeQualified(new Path(druidCluster.getDeepStorageDir()));
      fs.mkdirs(druidDeepStorage);
      final Path scratchDir =
          fs.makeQualified(new Path(QTestSystemProperties.getTempDir(), "druidStagingDir"));
      fs.mkdirs(scratchDir);
      conf.set("hive.druid.working.directory", scratchDir.toUri().getPath());
      druidCluster.init(conf);
      druidCluster.start();
    }

    if (clusterType == MiniClusterType.kafka || clusterType == MiniClusterType.druidKafka) {
      kafkaCluster =
          new SingleNodeKafkaCluster("kafka", QTestSystemProperties.getTempDir() + "/kafka-cluster",
              setup.zkPort, clusterType == MiniClusterType.kafka ? 9093 : 9092);
      kafkaCluster.init(conf);
      kafkaCluster.start();
      kafkaCluster.createTopicWithData("test-topic", new File(scriptsDir, "kafka_init_data.json"));
      kafkaCluster.createTopicWithData("wiki_kafka_csv",
          new File(scriptsDir, "kafka_init_data.csv"));
      kafkaCluster.createTopicWithData("wiki_kafka_avro_table", getAvroRows());
    }

    if (clusterType.getCoreClusterType() == CoreClusterType.TEZ) {
      if (confDir != null && !confDir.isEmpty()) {
        conf.addResource(
            new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
      }
      int numTrackers = 2;
      if (EnumSet
          .of(MiniClusterType.llap, MiniClusterType.llap_local, MiniClusterType.druidLocal,
              MiniClusterType.druidKafka, MiniClusterType.druid, MiniClusterType.kafka)
          .contains(clusterType)) {
        llapCluster = LlapItUtils.startAndGetMiniLlapCluster(conf, setup.zooKeeperCluster, confDir);
      }
      if (EnumSet
          .of(MiniClusterType.llap_local, MiniClusterType.tez_local, MiniClusterType.druidLocal)
          .contains(clusterType)) {
        mr = shims.getLocalMiniTezCluster(conf,
            clusterType == MiniClusterType.llap_local || clusterType == MiniClusterType.druidLocal);
      } else {
        mr = shims
            .getMiniTezCluster(conf, numTrackers, uriString,
                EnumSet
                    .of(MiniClusterType.llap, MiniClusterType.llap_local,
                        MiniClusterType.druidKafka, MiniClusterType.druid, MiniClusterType.kafka)
                    .contains(clusterType));
      }
    } else if (clusterType == MiniClusterType.miniSparkOnYarn) {
      mr = shims.getMiniSparkCluster(conf, 2, uriString, 1);
    } else if (clusterType == MiniClusterType.mr) {
      mr = shims.getMiniMrCluster(conf, 2, uriString, 1);
    }
  }

  public void shutDown() {
    if (druidCluster != null) {
      druidCluster.stop();
      druidCluster = null;
    }

    if (kafkaCluster != null) {
      kafkaCluster.stop();
      kafkaCluster = null;
    }
    setup.tearDown();
    if (sparkSession != null) {
      try {
        SparkSessionManagerImpl.getInstance().closeSession(sparkSession);
      } catch (Exception ex) {
        LOG.error("Error closing spark session.", ex);
      } finally {
        sparkSession = null;
      }
    }
    if (mr != null) {
      mr.shutdown();
      mr = null;
    }
    FileSystem.closeAll();
    if (dfs != null) {
      dfs.shutdown();
      dfs = null;
    }
  }

  public void initConf(HiveConf conf) throws IOException {
    if (mr != null) {
      mr.setupConfiguration(conf);

      // TODO Ideally this should be done independent of whether mr is setup or not.
      setFsRelatedProperties(conf, fs.getScheme().equals("file"), fs);
    }

    if (llapCluster != null) {
      Configuration clusterSpecificConf = llapCluster.getClusterSpecificConfiguration();
      for (Map.Entry<String, String> confEntry : clusterSpecificConf) {
        // Conf.get takes care of parameter replacement, iterator.value does not.
        conf.set(confEntry.getKey(), clusterSpecificConf.get(confEntry.getKey()));
      }
    }
    if (druidCluster != null) {
      final Path druidDeepStorage = fs.makeQualified(new Path(druidCluster.getDeepStorageDir()));
      fs.mkdirs(druidDeepStorage);
      conf.set("hive.druid.storage.storageDirectory", druidDeepStorage.toUri().getPath());
      conf.set("hive.druid.metadata.db.type", "derby");
      conf.set("hive.druid.metadata.uri", druidCluster.getMetadataURI());
      conf.set("hive.druid.coordinator.address.default", druidCluster.getCoordinatorURI());
      conf.set("hive.druid.overlord.address.default", druidCluster.getOverlordURI());
      conf.set("hive.druid.broker.address.default", druidCluster.getBrokerURI());
      final Path scratchDir =
          fs.makeQualified(new Path(QTestSystemProperties.getTempDir(), "druidStagingDir"));
      fs.mkdirs(scratchDir);
      conf.set("hive.druid.working.directory", scratchDir.toUri().getPath());
    }
  }

  public void setSparkSession(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public HadoopShims.HdfsEncryptionShim getHdfsEncryptionShim() {
    return hes;
  }

  public HadoopShims.MiniMrShim getMr() {
    return mr;
  }

  public void createRemoteDirs(HiveConf conf) {
    // Create remote dirs once.
    if (getMr() != null) {
      assert fs != null;
      Path warehousePath = fs.makeQualified(new Path(conf.getVar(ConfVars.METASTOREWAREHOUSE)));
      assert warehousePath != null;
      Path hiveJarPath = fs.makeQualified(new Path(conf.getVar(ConfVars.HIVE_JAR_DIRECTORY)));
      assert hiveJarPath != null;
      Path userInstallPath =
          fs.makeQualified(new Path(conf.getVar(ConfVars.HIVE_USER_INSTALL_DIR)));
      assert userInstallPath != null;
      try {
        fs.mkdirs(warehousePath);
      } catch (IOException e) {
        LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
            e.getMessage());
      }
      try {
        fs.mkdirs(hiveJarPath);
      } catch (IOException e) {
        LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
            e.getMessage());
      }
      try {
        fs.mkdirs(userInstallPath);
      } catch (IOException e) {
        LOG.error("Failed to create path={}. Continuing. Exception message={}", warehousePath,
            e.getMessage());
      }
    }
  }

  private void setupFileSystem(FsType fsType, HiveConf conf) throws IOException {
    if (fsType == FsType.local) {
      fs = FileSystem.getLocal(conf);
    } else if (fsType == FsType.hdfs || fsType == FsType.encrypted_hdfs
        || fsType == FsType.erasure_coded_hdfs) {
      int numDataNodes = 4;

      // Setup before getting dfs
      switch (fsType) {
      case encrypted_hdfs:
        // Set the security key provider so that the MiniDFS cluster is initialized
        // with encryption
        conf.set(SECURITY_KEY_PROVIDER_URI_NAME, getKeyProviderURI());
        conf.setInt("fs.trash.interval", 50);
        break;
      case erasure_coded_hdfs:
        // We need more NameNodes for EC.
        // To fully exercise hdfs code paths we need 5 NameNodes for the RS-3-2-1024k policy.
        // With 6 NameNodes we can also run the RS-6-3-1024k policy.
        numDataNodes = 6;
        break;
      default:
        break;
      }

      dfs = shims.getMiniDfs(conf, numDataNodes, true, null);
      fs = dfs.getFileSystem();

      // Setup after getting dfs
      switch (fsType) {
      case encrypted_hdfs:
        // set up the java key provider for encrypted hdfs cluster
        hes = shims.createHdfsEncryptionShim(fs, conf);
        LOG.info("key provider is initialized");
        break;
      case erasure_coded_hdfs:
        // The Erasure policy can't be set in a q_test_init script as QTestUtil runs that code in
        // a mode that disallows test-only CommandProcessors.
        // Set the default policy on the root of the file system here.
        HdfsErasureCodingShim erasureCodingShim = shims.createHdfsErasureCodingShim(fs, conf);
        erasureCodingShim.enableErasureCodingPolicy(DEFAULT_TEST_EC_POLICY);
        erasureCodingShim.setErasureCodingPolicy(new Path("hdfs:///"), DEFAULT_TEST_EC_POLICY);
        break;
      default:
        break;
      }
    } else {
      throw new IllegalArgumentException("Unknown or unhandled fsType [" + fsType + "]");
    }
  }

  private String getKeyProviderURI() {
    // Use the target directory if it is not specified
    String HIVE_ROOT = AbstractCliConfig.HIVE_ROOT;
    String keyDir = HIVE_ROOT + "ql/target/";

    // put the jks file in the current test path only for test purpose
    return "jceks://file" + new Path(keyDir, "test.jks").toUri();
  }

  private static List<byte[]> getAvroRows() {
    int numRows = 10;
    List<byte[]> events;
    final DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(Wikipedia.getClassSchema());
    events = IntStream.rangeClosed(0, numRows)
        .mapToObj(i -> Wikipedia.newBuilder()
            // 1534736225090 -> 08/19/2018 20:37:05
            .setTimestamp(formatter.format(new Timestamp(1534736225090L + 1000 * 3600 * i)))
            .setAdded(i * 300).setDeleted(-i).setIsrobot(i % 2 == 0)
            .setChannel("chanel number " + i).setComment("comment number " + i).setCommentlength(i)
            .setDiffurl(String.format("url %s", i)).setFlags("flag").setIsminor(i % 2 > 0)
            .setIsanonymous(i % 3 != 0).setNamespace("namespace")
            .setIsunpatrolled(new Boolean(i % 3 == 0)).setIsnew(new Boolean(i % 2 > 0))
            .setPage(String.format("page is %s", i * 100)).setDelta(i).setDeltabucket(i * 100.4)
            .setUser("test-user-" + i).build())
        .map(genericRecord -> {
          java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
          BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
          try {
            writer.write(genericRecord, encoder);
            encoder.flush();
            out.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return out.toByteArray();
        }).collect(Collectors.toList());
    return events;
  }

  private void setFsRelatedProperties(HiveConf conf, boolean isLocalFs, FileSystem fs) {
    String fsUriString = fs.getUri().toString();

    // Different paths if running locally vs a remote fileSystem. Ideally this difference should not
    // exist.
    Path warehousePath;
    Path jarPath;
    Path userInstallPath;
    if (isLocalFs) {
      String buildDir = QTestSystemProperties.getBuildDir();
      Preconditions.checkState(Strings.isNotBlank(buildDir));
      Path path = new Path(fsUriString, buildDir);

      // Create a fake fs root for local fs
      Path localFsRoot = new Path(path, "localfs");
      warehousePath = new Path(localFsRoot, "warehouse");
      jarPath = new Path(localFsRoot, "jar");
      userInstallPath = new Path(localFsRoot, "user_install");
    } else {
      // TODO Why is this changed from the default in hive-conf?
      warehousePath = new Path(fsUriString, "/build/ql/test/data/warehouse/");
      jarPath = new Path(new Path(fsUriString, "/user"), "hive");
      userInstallPath = new Path(fsUriString, "/user");
    }

    warehousePath = fs.makeQualified(warehousePath);
    jarPath = fs.makeQualified(jarPath);
    userInstallPath = fs.makeQualified(userInstallPath);

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUriString);

    // Remote dirs
    conf.setVar(ConfVars.METASTOREWAREHOUSE, warehousePath.toString());
    conf.setVar(ConfVars.HIVE_JAR_DIRECTORY, jarPath.toString());
    conf.setVar(ConfVars.HIVE_USER_INSTALL_DIR, userInstallPath.toString());
    // ConfVars.SCRATCHDIR - {test.tmp.dir}/scratchdir

    // Local dirs
    // ConfVars.LOCALSCRATCHDIR - {test.tmp.dir}/localscratchdir

    // TODO Make sure to cleanup created dirs.
  }
}

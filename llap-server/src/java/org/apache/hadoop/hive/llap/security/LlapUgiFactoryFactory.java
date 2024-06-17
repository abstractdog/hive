/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.security;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.UgiFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** No Java application is complete until it has a FactoryFactory. */
public class LlapUgiFactoryFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LlapUgiFactoryFactory.class);

  private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

  private static class KerberosUgiFactory implements UgiFactory {
    private final UserGroupInformation baseUgi;

    public KerberosUgiFactory(String keytab, String principal) throws IOException {
      baseUgi = LlapUtil.loginWithKerberos(principal, keytab);
    }

    @Override
    public UserGroupInformation createUgi(String queryIdentifier, String user, Credentials credentials) throws IOException {
      // Make sure the UGI is current.
      baseUgi.checkTGTAndReloginFromKeytab();
      // TODO: the only reason this is done this way is because we want unique Subject-s so that
      //       the FS.get gives different FS objects to different fragments.
      // TODO: could we log in from ticket cache instead? no good method on UGI right now
      UserGroupInformation ugi = SHIMS.cloneUgi(baseUgi);
      ugi.addCredentials(credentials);
      return ugi;
    }

    @Override
    public void closeAllFileSystemsForDag(String queryIdentifier) {}
  }

  private static class NoopUgiFactory implements UgiFactory {
    Map<String, UserGroupInformation> ugis = new HashMap<>();

    /**
     * Creates an ugi for tasks in the same query/dag and merges the credentials.
     * This is valid to be done once per dag: no vertex-level ugi and credentials are needed, both of them
     * are the same within the same dag.
     * Regarding vertex user: LlapTaskCommunicator has a single "user" field
     *   which is passed into the SignableVertexSpec
     * Regarding credentials: LlapTaskCommunicator creates SubmitWorkRequestProto instances
     *   into which dag-level credentials are passed
     */
    @Override
    public UserGroupInformation createUgi(String queryIdentifier, String user, Credentials credentials) throws IOException {
      if (ugis.containsKey(queryIdentifier)){
        UserGroupInformation ugi = ugis.get(queryIdentifier);
        LOG.info("Ugi already exists for query {}/{}", queryIdentifier, ugi);
        return ugi;
      }
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      ugi.addCredentials(credentials);
      ugis.put(queryIdentifier, ugi);
      LOG.info("Created ugi: {} for query/user {}/{}, current ugis #: {}", ugi, queryIdentifier, user, ugis.size());
      return ugi;
    }

    @Override
    public void closeAllFileSystemsForDag(String queryIdentifier) {
      LOG.info("Closing all ugis in NoopUgiFactory for query: {}", queryIdentifier);
      try {
        FileSystem.closeAllForUGI(ugis.get(queryIdentifier));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static UgiFactory createFsUgiFactory(Configuration conf) throws IOException {
    String fsKeytab = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_KEYTAB_FILE),
        fsPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_FS_KERBEROS_PRINCIPAL);
    boolean hasFsKeytab = fsKeytab != null && !fsKeytab.isEmpty(),
        hasFsPrincipal = fsPrincipal != null && !fsPrincipal.isEmpty();
    if (hasFsKeytab != hasFsPrincipal) {
      throw new IOException("Inconsistent FS keytab settings " + fsKeytab + "; " + fsPrincipal);
    }
    return hasFsKeytab ? new KerberosUgiFactory(fsKeytab, fsPrincipal) : new NoopUgiFactory();
  }
}

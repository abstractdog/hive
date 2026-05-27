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
package org.apache.hive.testutils.junit.extensions;

import org.apache.hive.testutils.ThreadDumpUtils;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.File;
import java.io.IOException;

/**
 * JUnit 4 RunListener that captures a full thread dump when a test fails.
 *
 * <p>This is the JUnit 4 variant needed for modules that only have {@code junit:junit} on their
 * test classpath (e.g. standalone-metastore). Surefire 3.x selects the surefire-junit4 provider
 * for such modules, and the only way to register a listener there is via the surefire
 * {@code <properties><property><name>listener</name>} configuration, which expects a
 * {@link RunListener} subclass.
 *
 * <p>Modules that have {@code junit-vintage-engine} or {@code junit-jupiter-engine} on their
 * classpath run under the JUnit Platform provider instead, where the surefire {@code listener}
 * property is ignored. Those modules are served by
 * {@link ThreadDumpOnFailureListener} which implements
 * {@code org.junit.platform.launcher.TestExecutionListener} and is auto-discovered via
 * {@code META-INF/services}.
 *
 * <p>Both listeners delegate to {@link org.apache.hive.testutils.ThreadDumpUtils} for the
 * actual thread dump capture and file writing.
 *
 * @see ThreadDumpOnFailureListener
 */
public class ThreadDumpOnFailureRunListener extends RunListener {

  @Override
  public void testFailure(Failure failure) throws Exception {
    String testClassName = failure.getDescription().getTestClass() != null
        ? failure.getDescription().getTestClass().getSimpleName()
        : failure.getDescription().getDisplayName();
    String testMethodName = failure.getDescription().getMethodName();

    File targetDir = resolveTargetDir();
    try {
      ThreadDumpUtils.writeThreadDumpToFile(targetDir, testClassName, testMethodName);
    } catch (IOException e) {
      System.err.println("Failed to write thread dump file: " + e.getMessage());
    }
  }

  private File resolveTargetDir() {
    String buildDir = System.getProperty("build.dir");
    if (buildDir != null) {
      return new File(buildDir);
    }
    return new File("target");
  }
}

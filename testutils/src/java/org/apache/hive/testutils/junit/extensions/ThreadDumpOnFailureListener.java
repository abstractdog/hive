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
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import java.io.File;
import java.io.IOException;

/**
 * JUnit Platform TestExecutionListener that captures a full thread dump when a test fails.
 *
 * <p>This listener is auto-discovered via the Java ServiceLoader mechanism
 * ({@code META-INF/services/org.junit.platform.launcher.TestExecutionListener}) for modules
 * whose test classpath includes a JUnit Platform engine ({@code junit-vintage-engine} or
 * {@code junit-jupiter-engine}). Surefire 3.x uses the JUnit Platform provider for such modules,
 * which honors ServiceLoader-based listener registration but ignores the surefire
 * {@code <properties><property><name>listener</name>} configuration meant for the older
 * surefire-junit4 provider.
 *
 * <p>Modules that only depend on {@code junit:junit} (without any Platform engine) are handled
 * by the surefire-junit4 provider, which requires a {@link org.junit.runner.notification.RunListener}
 * registered via surefire's {@code listener} property. That case is covered by
 * {@link ThreadDumpOnFailureRunListener}.
 *
 * <p>Two separate classes are necessary because the two providers have incompatible listener
 * interfaces and discovery mechanisms — there is no single class that works for both.
 *
 * <p>Both listeners delegate to {@link org.apache.hive.testutils.ThreadDumpUtils} for the
 * actual thread dump capture and file writing.
 *
 * @see ThreadDumpOnFailureRunListener
 */
public class ThreadDumpOnFailureListener implements TestExecutionListener {

  @Override
  public void executionFinished(TestIdentifier testIdentifier,
      TestExecutionResult testExecutionResult) {
    if (!testIdentifier.isTest()) {
      return;
    }
    if (testExecutionResult.getStatus() != TestExecutionResult.Status.FAILED) {
      return;
    }

    String testClassName = extractClassName(testIdentifier);
    String testMethodName = extractMethodName(testIdentifier);
    File targetDir = resolveTargetDir();
    try {
      ThreadDumpUtils.writeThreadDumpToFile(targetDir, testClassName, testMethodName);
    } catch (IOException e) {
      System.err.println("Failed to write thread dump file: " + e.getMessage());
    }
  }

  private String extractClassName(TestIdentifier testIdentifier) {
    TestSource source = testIdentifier.getSource().orElse(null);
    if (source instanceof MethodSource) {
      String className = ((MethodSource) source).getClassName();
      int dot = className.lastIndexOf('.');
      return dot >= 0 ? className.substring(dot + 1) : className;
    }
    if (source instanceof ClassSource) {
      return ((ClassSource) source).getJavaClass().getSimpleName();
    }
    return testIdentifier.getDisplayName();
  }

  private String extractMethodName(TestIdentifier testIdentifier) {
    TestSource source = testIdentifier.getSource().orElse(null);
    if (source instanceof MethodSource) {
      return ((MethodSource) source).getMethodName();
    }
    return null;
  }

  private File resolveTargetDir() {
    String buildDir = System.getProperty("build.dir");
    if (buildDir != null) {
      return new File(buildDir);
    }
    return new File("target");
  }
}

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
package org.apache.hive.testutils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public final class ThreadDumpUtils {

  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

  private ThreadDumpUtils() {
  }

  public static String getAllThreadStacksAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState());
      sb.append(System.lineSeparator());
      for (StackTraceElement frame : entry.getValue()) {
        sb.append("    at ").append(frame).append(System.lineSeparator());
      }
    }
    return sb.toString();
  }

  public static void writeThreadDumpToFile(File targetDir, String testClassName,
      String testMethodName) throws IOException {
    String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
    String testId = testMethodName != null
        ? testClassName + "_" + testMethodName : testClassName;
    String fileName = "threaddump-" + testId + "-" + timestamp + ".txt";
    File outputFile = new File(targetDir, fileName);
    targetDir.mkdirs();
    String content = "Thread dump for " + testId + " at " + timestamp
        + System.lineSeparator() + System.lineSeparator() + getAllThreadStacksAsString();
    Files.write(outputFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
  }
}

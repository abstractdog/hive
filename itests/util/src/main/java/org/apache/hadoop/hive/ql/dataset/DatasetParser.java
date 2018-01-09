/**
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
package org.apache.hadoop.hive.ql.dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DatasetParser {

  private DatasetCollection datasets = new DatasetCollection();
  public static final String DATASET_PREFIX = "--! qt:dataset:";

  public void parse(File file)  {
    try (BufferedReader br = new BufferedReader(new FileReader(file))){
      for (String line = br.readLine(); line != null; line = br.readLine()) {
         if(line.trim().startsWith(DATASET_PREFIX)){
           Set<String> strDatasets = parseDatasetsFromLine(line);
           
           for (String strDataset : strDatasets){
             datasets.add(strDataset);
           }
         }
      }
    }catch(IOException e){
      throw new RuntimeException("cannot find qfile while parsing for datasets", e);
    }
  }

  public DatasetCollection getDatasets(){
    return datasets;
  }
  
  public static Set<String> parseDatasetsFromLine(String input) {
    Set<String> datasets = new HashSet<String>();
    
    input = input.substring(DATASET_PREFIX.length());
    if (!input.trim().isEmpty()){
      datasets.addAll(Arrays.asList(input.split(",")));
    }

    return datasets;
  }
  
}

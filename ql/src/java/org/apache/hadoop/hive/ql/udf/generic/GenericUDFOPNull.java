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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IsNull;
import org.apache.hadoop.hive.ql.exec.vector.expressions.SelectColumnIsNull;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericUDFOPNull.
 *
 */
@Description(name = "isnull", value = "_FUNC_ a - Returns true if a is NULL and false otherwise")
@VectorizedExpressions({IsNull.class, SelectColumnIsNull.class})
public class GenericUDFOPNull extends GenericUDF {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDFOPNull.class.getName());
  private final BooleanWritable result = new BooleanWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].get() != null) {
        result.set(false);
        return result;
      }
    }
    result.set(true);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("IS NULL", children, ",");
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPNotNull();
  }
}

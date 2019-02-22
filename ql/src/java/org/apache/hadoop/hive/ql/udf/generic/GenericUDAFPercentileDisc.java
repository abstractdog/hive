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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDAFPercentileDisc.
 */
@Description(name = "percentile_disc", value = "_FUNC_(input, pc) - "
    + "Returns the percentile of expr at pc (range: [0,1]) without interpolation.")
public class GenericUDAFPercentileDisc extends GenericUDAFPercentileCont {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Exactly 2 argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case VOID:
      return new PercentileDiscLongEvaluator();
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
      return new PercentileDiscDoubleEvaluator();
    case STRING:
    case TIMESTAMP:
    case VARCHAR:
    case CHAR:
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
    }
  }

  /**
   * The evaluator for discrete percentile computation based on long.
   */
  public static class PercentileDiscLongEvaluator extends PercentileContLongEvaluator {
    PercentileDiscLongCalculator calc = new PercentileDiscLongCalculator();

    @Override
    protected void calculatePercentile(PercentileAgg percAgg,
        List<LongWritable> items) {
      long maxPosition = items.size() - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(calc.getPercentile(items, position));
    }
  }

  /**
   * The evaluator for discrete percentile computation based on double.
   */
  public static class PercentileDiscDoubleEvaluator extends PercentileContDoubleEvaluator {
    PercentileDiscDoubleCalculator calc = new PercentileDiscDoubleCalculator();

    @Override
    protected void calculatePercentile(PercentileAgg percAgg,
        List<DoubleWritable> items) {
      long maxPosition = items.size() - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(calc.getPercentile(items, position));
    }
  }

  /**
   * discrete percentile calculators
   */
  public abstract static class PercentileDiscCalculator<T> {
    abstract double getPercentile(List<T> items, double position);
  }

  public static class PercentileDiscLongCalculator extends PercentileDiscCalculator<LongWritable> {
    public double getPercentile(List<LongWritable> items,
        double position) {
      // We may need to do linear interpolation to get the exact percentile
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      // Linear search since this won't take much time from the total execution anyway
      // lower has the range of [0 .. total-1]
      // The first entry with accumulated count (lower+1) corresponds to the lower position.
      int i = 0;
      while (i < lower) {
        i++;
      }

      long lowerKey = items.get(i).get();
      if (higher == lower) {
        // no interpolation needed because position does not have a fraction
        return lowerKey;
      }

      if (i < higher + 1) {
        i++;
      }
      return items.get(i).get();
    }
  }

  public static class PercentileDiscDoubleCalculator
      extends PercentileDiscCalculator<DoubleWritable> {
    public double getPercentile(List<DoubleWritable> items,
        double position) {
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      int i = 0;
      while (i < lower) {
        i++;
      }

      double lowerKey = items.get(i).get();
      if (higher == lower) {
        return lowerKey;
      }

      if (i < higher + 1) {
        i++;
      }
      return items.get(i).get();
    }
  }
}

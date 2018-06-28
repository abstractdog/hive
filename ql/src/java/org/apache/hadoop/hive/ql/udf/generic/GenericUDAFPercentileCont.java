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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCorrelation.GenericUDAFCorrelationEvaluator.StdAgg;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;

@Description(name = "percentile_cont", value = "_FUNC_(pc) - Returns the percentile of expr at pc (range: [0,1]).")
public class GenericUDAFPercentileCont extends AbstractGenericUDAFResolver {

  private static final Comparator<LongWritable> COMPARATOR;

  static {
    COMPARATOR = ShimLoader.getHadoopShims().getLongComparator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
      throws SemanticException {
    GenericUDAFEvaluator eval = getEvaluator(paramInfo.getParameters());
    return eval;
  }

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
      return new PercentileContLongEvaluator();
    case TIMESTAMP:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case VARCHAR:
    case CHAR:
    case DECIMAL:
    case BOOLEAN:
    case DATE:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
    }
  }
  
  /**
   * A comparator to sort the entries in order.
   */
  public static class MyComparator implements Comparator<Map.Entry<LongWritable, LongWritable>> {
    @Override
    public int compare(Map.Entry<LongWritable, LongWritable> o1,
        Map.Entry<LongWritable, LongWritable> o2) {
      return COMPARATOR.compare(o1.getKey(), o2.getKey());
    }
  }

  /**
   * The evaluator for percentile computation based on long.
   */
  public static class PercentileContLongEvaluator extends GenericUDAFEvaluator {
    /**
     * A state class to store intermediate aggregation results.
     */
    public static class PercentileAgg extends AbstractAggregationBuffer {
      Map<LongWritable, LongWritable> counts;
      List<DoubleWritable> percentiles;
    }
    
    // For PARTIAL1 and COMPLETE
    protected PrimitiveObjectInspector inputOI;
    
    // For PARTIAL1 and PARTIAL2
    protected transient Object[] partialResult;
    private transient StructObjectInspector soi;
    
    // For FINAL and COMPLETE
    DoubleWritable result;
    
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      
      
      inputOI = (PrimitiveObjectInspector) parameters[0];

      // init output
      if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
        partialResult = new ObjectWritable[2];
        partialResult[0] = new MapWritable();
        partialResult[1] = new ArrayWritable(DoubleWritable.class);
       

        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

        ArrayList<String> fname = new ArrayList<String>();
        fname.add("counts");
        fname.add("percentiles");

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
      } else {
        result = new DoubleWritable(0);
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      PercentileAgg agg = new PercentileAgg();
      return agg;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      if (percAgg.counts != null) {
        percAgg.counts.clear();
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;

      Long input = PrimitiveObjectInspectorUtils.getLong(parameters[0], inputOI);
      
      HiveDecimalWritable percentile = (HiveDecimalWritable) parameters[1];
      Double dblPercentile = percentile.getHiveDecimal().doubleValue();

      if (percAgg.percentiles == null) {
        if (dblPercentile < 0.0 || dblPercentile > 1.0) {
          throw new RuntimeException("Percentile value must be within the range of 0 to 1.");
        }
        percAgg.percentiles = new ArrayList<DoubleWritable>(1);
        percAgg.percentiles.add(new DoubleWritable(dblPercentile));
      }
      if (input != null) {
        increment(percAgg, new LongWritable(input), 1);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      ((ObjectWritable) partialResult[0]).set(percAgg.counts);
      ((ObjectWritable) partialResult[1]).set(percAgg.percentiles);
      
      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;
      
      if (partial == null){
        return;
      }
      
      Object counts = soi.getStructFieldData(partial, countField);
      Object percentiles = soi.getStructFieldData(partial, xavgField);

//      if (percOther == null || percOther.counts == null || percOther.percentiles == null) {
//        return;
//      }
//
//      if (percOther.percentiles == null) {
//        percAgg.percentiles = new ArrayList<DoubleWritable>(percOther.percentiles);
//      }
//
//      for (Map.Entry<LongWritable, LongWritable> e : percOther.counts.entrySet()) {
//        increment(percAgg, e.getKey(), e.getValue().get());
//      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAgg percAgg = (PercentileAgg) agg;

      // No input data.
      if (percAgg.counts == null || percAgg.counts.size() == 0) {
        return null;
      }

      // Get all items into an array and sort them.
      Set<Map.Entry<LongWritable, LongWritable>> entries = percAgg.counts.entrySet();
      List<Map.Entry<LongWritable, LongWritable>> entriesList =
          new ArrayList<Map.Entry<LongWritable, LongWritable>>(entries);
      Collections.sort(entriesList, new MyComparator());

      // Accumulate the counts.
      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }

      // Initialize the result.
      if (result == null) {
        result = new DoubleWritable();
      }

      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(getPercentile(entriesList, position));

      return result;
    }

    /**
     * Increment the State object with o as the key, and i as the count.
     */
    private void increment(PercentileAgg s, LongWritable input, long i) {
      if (s.counts == null) {
        s.counts = new HashMap<LongWritable, LongWritable>();
      }
      LongWritable count = s.counts.get(input);
      if (count == null) {
        // We have to create a new object, because the object o belongs
        // to the code that creates it and may get its value changed.
        LongWritable key = new LongWritable(input.get());
        s.counts.put(key, new LongWritable(i));
      } else {
        count.set(count.get() + i);
      }
    }
  }

  /**
   * Get the percentile value.
   */
  private static double getPercentile(List<Map.Entry<LongWritable, LongWritable>> entriesList,
      double position) {
    // We may need to do linear interpolation to get the exact percentile
    long lower = (long) Math.floor(position);
    long higher = (long) Math.ceil(position);

    // Linear search since this won't take much time from the total execution anyway
    // lower has the range of [0 .. total-1]
    // The first entry with accumulated count (lower+1) corresponds to the lower position.
    int i = 0;
    while (entriesList.get(i).getValue().get() < lower + 1) {
      i++;
    }

    long lowerKey = entriesList.get(i).getKey().get();
    if (higher == lower) {
      // no interpolation needed because position does not have a fraction
      return lowerKey;
    }

    if (entriesList.get(i).getValue().get() < higher + 1) {
      i++;
    }
    long higherKey = entriesList.get(i).getKey().get();

    if (higherKey == lowerKey) {
      // no interpolation needed because lower position and higher position has the same key
      return lowerKey;
    }

    // Linear interpolation to get the exact percentile
    return (higher - position) * lowerKey + (position - lower) * higherKey;
  }
}

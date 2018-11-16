package org.apache.hadoop.hive.ql.udf.generic;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@Description(name = "percentile_disc", value = "_FUNC_(input, pc) - Returns the percentile of expr at pc (range: [0,1]) without interpolation.")
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
      return new PercentileDiscLongEvaluator();
    case FLOAT:
    case DOUBLE:
      return new PercentileDiscDoubleEvaluator();
    case STRING:
    case TIMESTAMP:
    case VARCHAR:
    case CHAR:
    case DECIMAL:
    case VOID:
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
        List<Map.Entry<LongWritable, LongWritable>> entriesList, long total) {
      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(calc.getPercentile(entriesList, position));
    }
  }

  /**
   * The evaluator for discrete percentile computation based on double.
   */
  public static class PercentileDiscDoubleEvaluator extends PercentileContDoubleEvaluator {
    PercentileDiscDoubleCalculator calc = new PercentileDiscDoubleCalculator();

    @Override
    protected void calculatePercentile(PercentileAgg percAgg,
        List<Map.Entry<DoubleWritable, LongWritable>> entriesList, long total) {
      // maxPosition is the 1.0 percentile
      long maxPosition = total - 1;
      double position = maxPosition * percAgg.percentiles.get(0).get();
      result.set(calc.getPercentile(entriesList, position));
    }
  }

  /**
   * continuous percentile calculators
   */
  public static abstract class PercentileDiscCalculator<T> {
    abstract double getPercentile(List<Map.Entry<T, LongWritable>> entriesList, double position);
  }

  public static class PercentileDiscLongCalculator extends PercentileDiscCalculator<LongWritable> {
    /**
     * Get the percentile value.
     */
    public double getPercentile(List<Map.Entry<LongWritable, LongWritable>> entriesList,
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
      return entriesList.get(i).getKey().get();
    }
  }

  public static class PercentileDiscDoubleCalculator
      extends PercentileDiscCalculator<DoubleWritable> {
    /**
     * Get the percentile value.
     */
    public double getPercentile(List<Map.Entry<DoubleWritable, LongWritable>> entriesList,
        double position) {
      long lower = (long) Math.floor(position);
      long higher = (long) Math.ceil(position);

      int i = 0;
      while (entriesList.get(i).getValue().get() < lower + 1) {
        i++;
      }

      double lowerKey = entriesList.get(i).getKey().get();
      if (higher == lower) {
        return lowerKey;
      }

      if (entriesList.get(i).getValue().get() < higher + 1) {
        i++;
      }
      return entriesList.get(i).getKey().get();
    }
  }
}

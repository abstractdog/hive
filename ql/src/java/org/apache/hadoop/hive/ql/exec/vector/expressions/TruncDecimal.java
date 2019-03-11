package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Vectorized implementation of trunc(number, scale) function for decimal input
 */
public class TruncDecimal extends TruncFloat {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  protected HiveDecimal pow = HiveDecimal.create(Math.pow(10, Math.abs(scale)));

  public TruncDecimal() {
    super();
  }

  public TruncDecimal(int colNum, int scale, int outputColumnNum) {
    super(colNum, scale, outputColumnNum);
  }

  @Override
  protected void trunc(ColumnVector inputColVector, ColumnVector outputColVector, int i) {
    BigDecimal input = BigDecimal
        .valueOf(((DecimalColumnVector) inputColVector).vector[i].getHiveDecimal().doubleValue());

    BigDecimal output = trunc(input);
    ((DecimalColumnVector) outputColVector).vector[i] =
        new HiveDecimalWritable(HiveDecimal.create(output));
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(VectorExpressionDescriptor.ArgumentType.DECIMAL,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;

public class TruncDateFromDate extends TruncDateFromTimestamp {
  private Date date = new Date();

  public TruncDateFromDate(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, fmt, outputColumnNum);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public TruncDateFromDate() {
    super();
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    truncDate((LongColumnVector) inV, outV, i);
  }

  protected void truncDate(LongColumnVector inV, BytesColumnVector outV, int i) {
    date = Date.ofEpochMilli(inV.vector[i]);
    processDate(outV, i, date);
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(VectorExpressionDescriptor.ArgumentType.DATE,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }

}

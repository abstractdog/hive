package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;

public class TruncDateFromString extends TruncDateFromTimestamp {
  private Date date = new Date();

  public TruncDateFromString(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, fmt, outputColumnNum);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public TruncDateFromString() {
    super();
    colNum = -1;
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    truncDate((BytesColumnVector) inV, outV, i);
  }

  protected void truncDate(BytesColumnVector inV, BytesColumnVector outV, int i) {
    if (inV.vector[i] == null) {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }

    String dateString =
        new String(inV.vector[i], inV.start[i], inV.length[i], StandardCharsets.UTF_8);
    if (dateParser.parseDate(dateString, date)) {
      processDate(outV, i, date);
    } else {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }

}

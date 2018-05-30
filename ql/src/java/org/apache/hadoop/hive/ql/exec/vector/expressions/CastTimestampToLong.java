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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MathExpr;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class CastTimestampToLong extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;

  private transient boolean integerPrimitiveCategoryKnown = false;
  private transient PrimitiveCategory integerPrimitiveCategory;

  public CastTimestampToLong(int colNum, int outputColumn) {
    this();
    this.colNum = colNum;
    this.outputColumn = outputColumn;
  }

  public CastTimestampToLong() {
    super();
  }

  private void setIntegerFromTimestamp(TimestampColumnVector inputColVector,
      LongColumnVector outputColVector, int batchIndex) {

    final long longValue = inputColVector.getTimestampAsLong(batchIndex);

    boolean isInRange;
    switch (integerPrimitiveCategory) {
    case BYTE:
      isInRange = ((byte) longValue) == longValue;
      break;
    case SHORT:
      isInRange = ((short) longValue) == longValue;
      break;
    case INT:
      isInRange = ((int) longValue) == longValue;
      break;
    case LONG:
      isInRange = true;
      break;
    default:
      throw new RuntimeException("Unexpected integer primitive category " + integerPrimitiveCategory);
    }
    if (isInRange) {
      outputColVector.vector[batchIndex] = longValue;
    } else {
      outputColVector.isNull[batchIndex] = true;
      outputColVector.noNulls = false;
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    if (!integerPrimitiveCategoryKnown) {
      String typeName = getOutputType().toLowerCase();
      if (typeName.equals("long")) {
        typeName = "bigint";
      }
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeName);
      integerPrimitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      integerPrimitiveCategoryKnown = true;
    }

    TimestampColumnVector inputColVector = (TimestampColumnVector) batch.cols[colNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        setIntegerFromTimestamp(inputColVector, outputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          final int i = sel[j];
          // Set isNull before call in case it changes it mind.
          outputIsNull[i] = false;
          setIntegerFromTimestamp(inputColVector, outputColVector, i);
        }
      } else {
        // Assume it is almost always a performance win to fill all of isNull so we can
        // safely reset noNulls.
        Arrays.fill(outputIsNull, false);
        outputColVector.noNulls = true;
        for(int i = 0; i != n; i++) {
          setIntegerFromTimestamp(inputColVector, outputColVector, i);
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!inputIsNull[i]) {
            outputIsNull[i] = false;
            setIntegerFromTimestamp(inputColVector, outputColVector, i);
          } else {
            outputIsNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputIsNull[i]) {
            outputIsNull[i] = false;
            setIntegerFromTimestamp(inputColVector, outputColVector, i);
          } else {
            outputIsNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "long";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("timestamp"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}

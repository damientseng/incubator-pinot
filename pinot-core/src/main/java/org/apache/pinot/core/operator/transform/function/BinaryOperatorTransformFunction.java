/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * <code>BinaryOperatorTransformFunction</code> abstracts common functions for binary operators (=, !=, >=, >, <=, <)
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 *
 */
public abstract class BinaryOperatorTransformFunction extends BaseTransformFunction {

  protected TransformFunction _leftTransformFunction;
  protected TransformFunction _rightTransformFunction;
  protected int[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exact 2 arguments are required for greater transform function");
    }
    _leftTransformFunction = arguments.get(0);
    _rightTransformFunction = arguments.get(1);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  protected void fillResultArray(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    FieldSpec.DataType dataType = _leftTransformFunction.getResultMetadata().getDataType();
    int length = projectionBlock.getNumDocs();
    switch (dataType) {
      case INT:
        int[] leftIntValues = _leftTransformFunction.transformToIntValuesSV(projectionBlock);
        int[] rightIntValues = _rightTransformFunction.transformToIntValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] = getBinaryFuncResult(((Integer) leftIntValues[i]).compareTo(rightIntValues[i]));
        }
        break;
      case LONG:
        long[] leftLongValues = _leftTransformFunction.transformToLongValuesSV(projectionBlock);
        long[] rightLongValues = _rightTransformFunction.transformToLongValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] = getBinaryFuncResult(((Long) leftLongValues[i]).compareTo(rightLongValues[i]));
        }
        break;
      case FLOAT:
        float[] leftFloatValues = _leftTransformFunction.transformToFloatValuesSV(projectionBlock);
        float[] rightFloatValues = _rightTransformFunction.transformToFloatValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] = getBinaryFuncResult(((Float) leftFloatValues[i]).compareTo(rightFloatValues[i]));
        }
        break;
      case DOUBLE:
        double[] leftDoubleValues = _leftTransformFunction.transformToDoubleValuesSV(projectionBlock);
        double[] rightDoubleValues = _rightTransformFunction.transformToDoubleValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] = getBinaryFuncResult(((Double) leftDoubleValues[i]).compareTo(rightDoubleValues[i]));
        }
        break;
      case STRING:
        String[] leftStringValues = _leftTransformFunction.transformToStringValuesSV(projectionBlock);
        String[] rightStringValues = _rightTransformFunction.transformToStringValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] = getBinaryFuncResult(leftStringValues[i].compareTo(rightStringValues[i]));
        }
        break;
      case BYTES:
        byte[][] leftBytesValues = _leftTransformFunction.transformToBytesValuesSV(projectionBlock);
        byte[][] rightBytesValues = _rightTransformFunction.transformToBytesValuesSV(projectionBlock);
        for (int i = 0; i < length; i++) {
          _results[i] =
              getBinaryFuncResult((new ByteArray(leftBytesValues[i])).compareTo(new ByteArray(rightBytesValues[i])));
        }
        break;
      // NOTE: Multi-value columns are not comparable, so we should not reach here
      default:
        throw new IllegalStateException();
    }
  }

  abstract int getBinaryFuncResult(int result);
}

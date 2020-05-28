package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;


/**
 * The <code>EqualsTransformFunction</code> extends <code>BinaryOperatorTransformFunction</code> to implement the
 * binary operator(=).
 *
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 *
 * SQL Syntax:
 *    columnA = 12
 *    columnA = 12.0
 *    columnA = 'fooBar'
 *
 * Sample Usage:
 *    EQUALS(columnA, 12)
 *    EQUALS(columnA, 12.0)
 *    EQUALS(columnA, 'fooBar')
 *
 */
public class EqualsTransformFunction extends BinaryOperatorTransformFunction {

  @Override
  public String getName() {
    return TransformFunctionType.EQUALS.getName();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    fillResultArray(projectionBlock);
    return _results;
  }

  @Override
  int getBinaryFuncResult(int result) {
    return (result == 0) ? 1 : 0;
  }
}
/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.segment.ColumnValueSelector;

public class NullableDoubleAggregateCombiner extends DoubleAggregateCombiner
{
  private boolean isNull;

  private final DoubleAggregateCombiner delegate;

  public NullableDoubleAggregateCombiner(DoubleAggregateCombiner delegate)
  {
    this.delegate = delegate;
    this.isNull = true;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    isNull = true;
    delegate.reset(selector);
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    if (isNull && !selector.isNull()) {
      isNull = false;
    }
    delegate.fold(selector);
  }

  @Override
  public double getDouble()
  {
    return delegate.getDouble();
  }

  @Override
  public boolean isNull()
  {
    return isNull;
  }
}

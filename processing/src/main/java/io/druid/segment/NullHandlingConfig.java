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

package io.druid.segment;

import com.google.common.base.Strings;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.DoubleAggregateCombiner;
import io.druid.query.aggregation.LongAggregateCombiner;
import io.druid.query.aggregation.NullFilteringAggregator;
import io.druid.query.aggregation.NullFilteringBufferAggregator;
import io.druid.query.aggregation.NullableAggregator;
import io.druid.query.aggregation.NullableBufferAggregator;
import io.druid.query.aggregation.NullableDoubleAggregateCombiner;
import io.druid.query.aggregation.NullableLongAggregateCombiner;
import io.druid.query.aggregation.NullableObjectAggregateCombiner;
import io.druid.query.aggregation.ObjectAggregateCombiner;

public interface NullHandlingConfig
{

  public static final NullHandlingConfig LEGACY_CONFIG = new NullHandlingConfig(){

    @Override
    public boolean useDefaultValuesForNull()
    {
      return true;
    }
  };

  boolean useDefaultValuesForNull();

  default String getDefaultOrNull(String value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? Strings.nullToEmpty(value) : value;
  }

  default String emptyToNull(String value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? Strings.emptyToNull(value) : value;
  }

  default boolean isNullOrDefault(String value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? Strings.isNullOrEmpty(value) : value == null;
  }

  default Long getDefaultOrNull(Long value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? 0L : value;
  }

  default Double getDefaultOrNull(Double value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? 0.0D : value;
  }

  default Float getDefaultOrNull(Float value){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? 0.0F : value;
  }

  default Aggregator getNullableAggregator(Aggregator aggregator, ColumnValueSelector selector){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? aggregator : new NullableAggregator(aggregator, selector);
  }

  default Aggregator getNullFilteringAggregator(Aggregator aggregator, ColumnValueSelector selector){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? aggregator : new NullFilteringAggregator(aggregator, selector);
  }

  default BufferAggregator getNullFilteringAggregator(BufferAggregator aggregator, ColumnValueSelector selector){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? aggregator : new NullFilteringBufferAggregator(aggregator, selector);
  }

  default BufferAggregator getNullableAggregator(BufferAggregator aggregator, ColumnValueSelector selector){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  default DoubleAggregateCombiner getNullableCombiner(DoubleAggregateCombiner combiner){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? combiner : new NullableDoubleAggregateCombiner(combiner);
  }

  default LongAggregateCombiner getNullableCombiner(LongAggregateCombiner combiner){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? combiner : new NullableLongAggregateCombiner(combiner);
  }

  default ObjectAggregateCombiner getNullableCombiner(ObjectAggregateCombiner combiner){
    return NullHandlingConfig.this.useDefaultValuesForNull() ? combiner : new NullableObjectAggregateCombiner(combiner);
  }


}

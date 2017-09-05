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
import com.google.inject.Inject;
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

public class NullHandlingHelper
{
  // Using static injection to avoid adding JacksonInject annotations all over the code place.
  @Inject
  private static NullValueHandlingConfig INSTANCE;

  public static boolean useDefaultValuesForNull(){
    return INSTANCE.isUseDefaultValuesForNull();
  }

  public static String getDefaultOrNull(String value){
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.nullToEmpty(value) : value;
  }

  public static String defaultToNull(String value){
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.emptyToNull(value) : value;
  }

  public static boolean isNullOrDefault(String value){
    return INSTANCE.isUseDefaultValuesForNull() ? Strings.isNullOrEmpty(value) : value == null;
  }

  public static Long getDefaultOrNull(Long value){
    return INSTANCE.isUseDefaultValuesForNull() ? 0L : value;
  }

  public static Double getDefaultOrNull(Double value){
    return INSTANCE.isUseDefaultValuesForNull() ? 0.0D : value;
  }

  public static Float getDefaultOrNull(Float value){
    return INSTANCE.isUseDefaultValuesForNull() ? 0.0F : value;
  }

  public static Aggregator getNullableAggregator(Aggregator aggregator, ColumnValueSelector selector){
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullableAggregator(aggregator, selector);
  }

  public static Aggregator getNullFilteringAggregator(Aggregator aggregator, ColumnValueSelector selector){
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullFilteringAggregator(aggregator, selector);
  }

  public static BufferAggregator getNullFilteringAggregator(BufferAggregator aggregator, ColumnValueSelector selector){
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullFilteringBufferAggregator(aggregator, selector);
  }

  public static BufferAggregator getNullableAggregator(BufferAggregator aggregator, ColumnValueSelector selector){
    return INSTANCE.isUseDefaultValuesForNull() ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  public static DoubleAggregateCombiner getNullableCombiner(DoubleAggregateCombiner combiner){
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableDoubleAggregateCombiner(combiner);
  }

  public static LongAggregateCombiner getNullableCombiner(LongAggregateCombiner combiner){
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableLongAggregateCombiner(combiner);
  }

  public static ObjectAggregateCombiner getNullableCombiner(ObjectAggregateCombiner combiner){
    return INSTANCE.isUseDefaultValuesForNull() ? combiner : new NullableObjectAggregateCombiner(combiner);
  }
}

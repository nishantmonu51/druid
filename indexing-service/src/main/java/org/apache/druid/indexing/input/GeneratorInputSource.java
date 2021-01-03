/*
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

package org.apache.druid.indexing.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputSourceSecurityConfig;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorColumnSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * {@link InputSource} that can be used to seed a Druid cluster with test data, using either the built-in schemas
 * defined in {@link GeneratorBasicSchemas}, or by directly supplying a list of {@link GeneratorColumnSchema}, to
 * construct a {@link DataGenerator}. To produce a stable set of data, a random {@link #seed} may be supplied which
 * will be used for all data generated by the columns. When {@link #numSplits} is greater than 1, the {@link #seed}
 * will be instead used to pick a new seed for each split, allowing the splits to produce a different set of data,
 * but still in a stable manner.
 */
public class GeneratorInputSource extends AbstractInputSource implements SplittableInputSource<Long>
{
  private static final int DEFAULT_NUM_ROWS = 1000;
  private static final int DEFAULT_NUM_SPLITS = 1;
  private static final long DEFAULT_SEED = 1024L;
  private static final long DEFAULT_START_TIME = DateTimes.nowUtc().minusDays(1).getMillis();
  private static final int DEFAULT_CONSECUTIVE_TIMESTAMPS = 100;
  private static final double DEFAULT_TIMESTAMP_INCREMENT = 1.0;

  private final String schemaName;
  private final List<GeneratorColumnSchema> schema;
  private final int numRows;
  private final Integer numSplits;
  private final Long seed;
  private final Long startTime;
  private final Integer numConsecutiveTimestamps;
  private final Double timestampIncrement;
  
  @JsonCreator
  public GeneratorInputSource(
      @JsonProperty("schemaName") @Nullable String schemaName,
      @JsonProperty("schema") @Nullable List<GeneratorColumnSchema> schema,
      @JsonProperty("numRows") Integer numRows,
      @JsonProperty("numSplits") Integer numSplits,
      @JsonProperty("seed") Long seed,
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("numConsecutiveTimestamps") Integer numConsecutiveTimestamps,
      @JsonProperty("timestampIncrement") Double timestampIncrement
  )
  {
    Preconditions.checkArgument(
        schemaName != null || schema != null,
        "Must specify either 'schemaName' or 'schema'"
    );
    this.schemaName = schemaName;
    this.schema = schema != null
                         ? schema
                         : GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName).getColumnSchemas();
    this.numRows = numRows != null ? numRows : DEFAULT_NUM_ROWS;
    this.numSplits = numSplits != null ? numSplits : DEFAULT_NUM_SPLITS;
    this.seed = seed != null ? seed : DEFAULT_SEED;
    this.startTime = startTime != null ? startTime : DEFAULT_START_TIME;
    this.numConsecutiveTimestamps = numConsecutiveTimestamps != null
                                    ? numConsecutiveTimestamps
                                    : DEFAULT_CONSECUTIVE_TIMESTAMPS;
    this.timestampIncrement = timestampIncrement != null ? timestampIncrement : DEFAULT_TIMESTAMP_INCREMENT;
  }

  @Override
  public Stream<InputSplit<Long>> createSplits(
      InputFormat inputFormat,
      @Nullable SplitHintSpec splitHintSpec
  )
  {
    Random r = new Random(seed);
    return LongStream.range(0, numSplits).mapToObj(i -> new InputSplit<>(r.nextLong()));
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return numSplits;
  }

  @Override
  public InputSource withSplit(InputSplit<Long> split)
  {
    return new GeneratorInputSource(
        schemaName,
        schema,
        numRows,
        1,
        split.get(),
        startTime,
        numConsecutiveTimestamps,
        timestampIncrement
    );
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
  {
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read()
      {
        return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>()
        {
          int rowCount = 0;
          private final DataGenerator generator = makeGenerator();

          @Override
          public boolean hasNext()
          {
            return rowCount < numRows;
          }

          @Override
          public InputRow next()
          {
            rowCount++;
            return generator.nextRow();
          }
        });
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample()
      {
        return CloseableIterators.withEmptyBaggage(new Iterator<InputRowListPlusRawValues>()
        {
          int rowCount = 0;
          private final DataGenerator generator = makeGenerator();

          @Override
          public boolean hasNext()
          {
            return rowCount < numRows;
          }

          @Override
          public InputRowListPlusRawValues next()
          {
            rowCount++;
            InputRow row = generator.nextRow();
            return InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent());
          }
        });
      }
    };
  }

  @JsonProperty
  public String getSchemaName()
  {
    return schemaName;
  }

  @JsonProperty
  public List<GeneratorColumnSchema> getSchema()
  {
    return schemaName == null ? schema : null;
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public Integer getNumSplits()
  {
    return numSplits;
  }

  @JsonProperty
  public Long getSeed()
  {
    return seed;
  }

  @JsonProperty
  public Long getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public Integer getNumConsecutiveTimestamps()
  {
    return numConsecutiveTimestamps;
  }

  @JsonProperty
  public Double getTimestampIncrement()
  {
    return timestampIncrement;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeneratorInputSource that = (GeneratorInputSource) o;
    return numRows == that.numRows &&
           Objects.equals(schemaName, that.schemaName) &&
           Objects.equals(schema, that.schema) &&
           Objects.equals(numSplits, that.numSplits) &&
           Objects.equals(seed, that.seed) &&
           Objects.equals(startTime, that.startTime) &&
           Objects.equals(numConsecutiveTimestamps, that.numConsecutiveTimestamps) &&
           Objects.equals(timestampIncrement, that.timestampIncrement);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        schemaName,
        schema,
        numRows,
        numSplits,
        seed,
        startTime,
        numConsecutiveTimestamps,
        timestampIncrement
    );
  }

  private DataGenerator makeGenerator()
  {
    return new DataGenerator(
        schema,
        seed,
        startTime,
        numConsecutiveTimestamps,
        timestampIncrement
    );
  }

  @Override
  public void validateAllowDenyPrefixList(InputSourceSecurityConfig securityConfig)
  {
    // No URI to validate
    setValidated();
  }
}

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

package io.druid.segment.serde;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.DoubleColumnSerializer;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;
import io.druid.segment.data.IndexedDoubles;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DoubleGenericColumnPartSerdeV2 implements ColumnPartSerde
{
  private final ByteOrder byteOrder;
  private Serializer serialize;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  @JsonCreator
  public static DoubleGenericColumnPartSerdeV2 getDoubleGenericColumnPartSerde(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @Nullable @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new DoubleGenericColumnPartSerdeV2(byteOrder,
                                              bitmapSerdeFactory != null
                                              ? bitmapSerdeFactory
                                              : new BitmapSerde.LegacyBitmapSerdeFactory(), null
    );
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  public DoubleGenericColumnPartSerdeV2(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      Serializer serialize
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.serialize = serialize;
  }

  @Override
  public Serializer getSerializer()
  {
    return serialize;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return (buffer, builder, columnConfig) -> {
      int offset = buffer.getInt();
      int initialPos = buffer.position();
      final Supplier<IndexedDoubles> column = CompressedDoublesIndexedSupplier.fromByteBuffer(
          buffer,
          byteOrder
      );

      buffer.position(initialPos + offset);
      final ImmutableBitmap bitmap;
      if (buffer.hasRemaining()) {
        bitmap = ByteBufferSerializer.read(buffer, bitmapSerdeFactory.getObjectStrategy());
      } else {
        bitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
      }
      builder.setType(ValueType.DOUBLE)
             .setHasMultipleValues(false)
             .setGenericColumn(new DoubleGenericColumnSupplier(column, bitmap));
    };
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private DoubleColumnSerializer delegate = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final DoubleColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public DoubleGenericColumnPartSerdeV2 build()
    {
      return new DoubleGenericColumnPartSerdeV2(
          byteOrder,
          bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize() throws IOException
            {
              return delegate.getSerializedSize();
            }

            @Override
            public void writeTo(WritableByteChannel channel, FileSmoosher fileSmoosher) throws IOException
            {
              delegate.writeTo(channel, fileSmoosher);
            }
          }
      );
    }
  }
}

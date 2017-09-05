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
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.DoubleColumnSerializer;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DoubleGenericColumnPartSerdeV2 implements ColumnPartSerde
{
  static final byte VERSION_ONE = 0x1;
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
                                            bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(), null);
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }


  public DoubleGenericColumnPartSerdeV2(ByteOrder byteOrder, BitmapSerdeFactory bitmapSerdeFactory, Serializer serialize)
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
      byte versionFromBuffer = buffer.get();

      if (VERSION_ONE == versionFromBuffer) {

        int offset = buffer.getInt();
        int initialPos = buffer.position();
        final CompressedDoublesIndexedSupplier column = CompressedDoublesIndexedSupplier.fromByteBuffer(
            buffer,
            byteOrder,
            builder.getFileMapper()
        );

        buffer.position(initialPos + offset);
        final ImmutableBitmap bitmap;
        if (buffer.hasRemaining()) {
          int bitmapSize = buffer.getInt();
          bitmap = bitmapSerdeFactory.getObjectStrategy().fromByteBuffer(buffer, bitmapSize);
          builder.setNullValueBitmap(Suppliers.ofInstance(bitmap));
        } else {
          bitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
        }
        builder.setType(ValueType.DOUBLE)
               .setHasMultipleValues(false)
               .setGenericColumn(new DoubleGenericColumnSupplier(column, bitmap));
      } else {
        throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
      }
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
    private ByteBufferWriter<ImmutableBitmap> nullValueBitmapWriter = null;

    public
    SerializerBuilder withByteOrder(final ByteOrder byteOrder)
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

    public SerializerBuilder withNullValueBitmapWriter(ByteBufferWriter<ImmutableBitmap> nullValueBitmapWriter)
    {
      this.nullValueBitmapWriter = nullValueBitmapWriter;
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
            public long numBytes()
            {
              long size = delegate.getSerializedSize()+ Ints.BYTES + Byte.BYTES;
              if(nullValueBitmapWriter != null){
                size += nullValueBitmapWriter.getSerializedSize();
              }
              return size;
            }

            @Override
            public void write(WritableByteChannel channel, FileSmoosher fileSmoosher) throws IOException
            {
              channel.write(ByteBuffer.wrap(new byte[]{VERSION_ONE}));
              channel.write(ByteBuffer.wrap(Ints.toByteArray((int)delegate.getSerializedSize())));
              delegate.writeToChannel(channel, fileSmoosher);
              if(nullValueBitmapWriter != null){
                nullValueBitmapWriter.writeToChannel(channel, fileSmoosher);
              }
            }
          }
      );
    }
  }
}

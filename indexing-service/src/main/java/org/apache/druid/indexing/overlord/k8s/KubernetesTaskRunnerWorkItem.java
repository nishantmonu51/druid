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

package org.apache.druid.indexing.overlord.k8s;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import io.fabric8.kubernetes.api.model.HasMetadata;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.java.util.common.guava.CloseQuietly;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class KubernetesTaskRunnerWorkItem extends TaskRunnerWorkItem implements Closeable
{
  private final SettableFuture<TaskStatus> result;
  private final String dataSource;

  private String taskType;
  private TaskLocation location;
  private List<Closeable> closeables = new ArrayList<>();
  private State state;
  private List<HasMetadata> charts;


  public KubernetesTaskRunnerWorkItem(
      String taskId,
      String taskType,
      TaskLocation location,
      String dataSource,
      List<HasMetadata> charts
  )
  {
    this(taskId, taskType, SettableFuture.create(), location, dataSource, charts, State.PENDING);
  }

  private KubernetesTaskRunnerWorkItem(
      String taskId,
      String taskType,
      SettableFuture<TaskStatus> result,
      TaskLocation location,
      String dataSource,
      List<HasMetadata> charts,
      State state
  )
  {
    super(taskId, result);
    this.result = result;
    this.taskType = taskType;
    this.location = location == null ? TaskLocation.unknown() : location;
    this.dataSource = dataSource;
    this.charts = charts;
    this.state = state;
  }

  @Override
  public TaskLocation getLocation()
  {
    return location;
  }

  public void setLocation(TaskLocation location)
  {
    this.location = location;
  }

  @Override
  public String getTaskType()
  {
    return taskType;
  }

  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  public void setResult(TaskStatus status)
  {
    setState(State.COMPLETE);
    result.set(status);
  }

  public State getState()
  {
    return state;
  }

  public void setState(State state)
  {
    Preconditions.checkArgument(
        state.index - this.state.index > 0,
        "Invalid state transition from [%s] to [%s]",
        this.state,
        state
    );

    this.state = state;
  }

  public List<HasMetadata> getCharts()
  {
    return charts;
  }

  public void registerCloseable(Closeable closeable)
  {
    this.closeables.add(closeable);
  }

  @Override
  public void close()
  {
    for (Closeable closeable : closeables) {
      CloseQuietly.close(closeable);
    }
  }

  enum State
  {
    PENDING(0),
    RUNNING(1),
    COMPLETE(2);

    int index;

    State(int index)
    {
      this.index = index;
    }
  }
}

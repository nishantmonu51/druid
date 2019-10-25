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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.tasklogs.TaskLogPusher;

/**
 *
 */
public class KubernetesTaskRunnerFactory implements TaskRunnerFactory<KubernetesTaskRunner>
{
  public static final String TYPE_NAME = "kubernetes";
  private final KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private final ObjectMapper jsonMapper;
  private final TaskLogPusher taskLogPusher;

  @Inject
  public KubernetesTaskRunnerFactory(
      final KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      final ObjectMapper jsonMapper,
      final TaskLogPusher taskLogPusher
  )
  {
    this.kubernetesTaskRunnerConfig = kubernetesTaskRunnerConfig;
    this.jsonMapper = jsonMapper;
    this.taskLogPusher = taskLogPusher;
  }

  @Override
  public KubernetesTaskRunner build()
  {
    return new KubernetesTaskRunner(
        kubernetesTaskRunnerConfig,
        jsonMapper,
        taskLogPusher
    );
  }
}

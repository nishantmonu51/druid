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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Resource;
import me.snowdrop.istio.api.IstioResource;
import me.snowdrop.istio.client.DefaultIstioClient;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogStreamer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class KubernetesTaskRunner implements TaskLogStreamer, TaskRunner
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesTaskRunner.class);
  private final TaskLogPusher taskLogPusher;
  private final KubernetesClient client;
  private final KubernetesTaskRunnerConfig config;
  private final DefaultIstioClient istioClient;
  private final ObjectMapper jsonMapper;

  /**
   * Writes must be synchronized. This is only a ConcurrentMap so "informational" reads can occur without waiting.
   */
  private final ConcurrentMap<String, KubernetesTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  // Guards the pending/running/complete lists of tasks and list of workers
  // statusLock.notifyAll() is called whenever there is a possibility of worker slot to run task becoming available.
  // statusLock.notifyAll() is called whenever a task status or location changes.
  private final Object statusLock = new Object();

  public KubernetesTaskRunner(KubernetesTaskRunnerConfig config, ObjectMapper jsonMapper, TaskLogPusher taskLogPusher)
  {
    this.config = config;
    Config k8sConfig = new ConfigBuilder().build();
    client = new DefaultKubernetesClient(k8sConfig);
    this.jsonMapper = jsonMapper;
    this.istioClient = new DefaultIstioClient(k8sConfig);
    this.taskLogPusher = taskLogPusher;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return null;
  }

  @Override
  public void start()
  {

  }

  private static String sanitizeName(String name)
  {
    char[] chars = new char[]{'-', '_', '.', ':'};
    String str = name.toLowerCase(Locale.ROOT);
    for (char ch : chars) {
      str = StringUtils.removeChar(str, ch);
    }
    return str;
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);

    synchronized (tasks) {
      for (KubernetesTaskRunnerWorkItem item : tasks.values()) {
        TaskRunnerUtils.notifyLocationChanged(ImmutableList.of(listenerPair), item.getTaskId(), item.getLocation());
      }

      listeners.add(listenerPair);
      log.info("Registered listener [%s]", listener.getListenerId());
    }
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        log.info("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    synchronized (statusLock) {
      KubernetesTaskRunnerWorkItem existing = tasks.get(task.getId());
      if (existing != null) {
        log.info("Assigned a task[%s] that is known already. Ignored.", task.getId());
        return existing.getResult();
      }
      final String configMapName = "config-map" + sanitizeName(task.getId());
      final Resource<ConfigMap, DoneableConfigMap> configMapResource = client.configMaps()
                                                                             .inNamespace(config.getNamespace())
                                                                             .withName(sanitizeName(task.getId()));
      log.info(configMapName);

      try {
        configMapResource.createOrReplace(
            new ConfigMapBuilder()
                .withNewMetadata()
                .withName(configMapName)
                .withNamespace(config.getNamespace())
                .endMetadata()
                .addToData("task.json", jsonMapper.writeValueAsString(task))
                .build()
        );

        log.info("Trying to read from template file " + config.getTemplateFile());
        String template = IOUtils.toString(
            new FileInputStream(new File(config.getTemplateFile())),
            Charsets.UTF_8
        );
        template = StringUtils.replace(template, "TASK_ID", sanitizeName(task.getId()));
        template = StringUtils.replace(
            template,
            "AVAILABILITY_GROUP",
            sanitizeName(task.getTaskResource().getAvailabilityGroup())
        );

        final List<HasMetadata> charts = client.load(IOUtils.toInputStream(template, Charsets.UTF_8)).get();
        install(charts);
        KubernetesTaskRunnerWorkItem taskRunnerWorkItem = new KubernetesTaskRunnerWorkItem(
            task.getId(),
            task.getType(),
            TaskLocation
                .unknown(),
            task.getDataSource(),
            charts
        );

        tasks.put(task.getId(), taskRunnerWorkItem);
        Watch taskWatch = client.pods()
                                .inNamespace(config.getNamespace())
                                .withLabel("job-name=" + sanitizeName(task.getId()))
                                .watch(new Watcher<Pod>()
                                {
                                  @Override
                                  public void eventReceived(final Action action, Pod pod)
                                  {
                                    List<ContainerStatus> druidStatuses = pod.getStatus()
                                                                             .getContainerStatuses()
                                                                             .stream()
                                                                             .filter(containerStatus -> containerStatus.getName()
                                                                                                                       .equals(
                                                                                                                           "druid"))
                                                                             .collect(
                                                                                 Collectors.toList());
                                    if (druidStatuses.size() == 0) {
                                      log.info("WTF!! No Druid status found" + action + "pod" + pod);
                                      return;
                                    }
                                    ContainerStatus druidStatus = druidStatuses.get(0);

                                    log.info("Task Id " + task.getId() + "Status : " + druidStatus);
                                    if (druidStatus.getReady()) {
                                      if (taskRunnerWorkItem.getState() == KubernetesTaskRunnerWorkItem.State.PENDING) {
                                        log.info("Task[%s] moving to running state!", sanitizeName(task.getId()));
                                        taskRunnerWorkItem.setState(KubernetesTaskRunnerWorkItem.State.RUNNING);
                                      }
                                    } else {
                                      ContainerStateTerminated terminated = druidStatus.getState().getTerminated();
                                      if (terminated != null) {
                                        if (terminated.getExitCode() == 0) {
                                          taskRunnerWorkItem.setResult(TaskStatus.success(task.getId()));
                                        } else {
                                          taskRunnerWorkItem.setResult(TaskStatus.failure(task.getId()));
                                        }
                                        log.info("Task[%s] is completed!", task.getId());

                                        try {
                                          File logFile = File.createTempFile(task.getId(), ".log");
                                          Files.copy(
                                              new ReaderInputStream(client.pods()
                                                                          .inNamespace(config.getNamespace())
                                                                          .withName(pod.getMetadata()
                                                                                       .getName())
                                                                          .inContainer("druid")
                                                                          .getLogReader(), Charsets.UTF_8),
                                              logFile.toPath(),
                                              StandardCopyOption.REPLACE_EXISTING
                                          );

                                          taskLogPusher.pushTaskLog(task.getId(), logFile);
                                          tasks.remove(task.getId());
                                        }
                                        catch (IOException e) {
                                          throw new RuntimeException(e);
                                        }
                                        finally {
                                          CloseQuietly.close(taskRunnerWorkItem);
                                        }
                                      }
                                    }
                                  }

                                  @Override
                                  public void onClose(final KubernetesClientException e)
                                  {
                                    log.info("Cleaning up job " + sanitizeName(task.getId()));
                                    delete(charts);
                                    configMapResource.delete();
                                    try {
                                      client.pods()
                                            .inNamespace(config.getNamespace())
                                            .withLabel("job-name=" + sanitizeName(task.getId()))
                                            .delete();
                                    }
                                    catch (Exception e1) {
                                      log.error("Exception while cleaning up pod" + e1);
                                    }
                                  }
                                });
        taskRunnerWorkItem.registerCloseable(taskWatch);
        return taskRunnerWorkItem.getResult();
      }
      catch (IOException e) {
        throw new ISE("Exception while submitting task ", e);
      }
    }
  }

  private void install(List<HasMetadata> hasMetadata)
  {
    for (HasMetadata spec : hasMetadata) {
      if (spec instanceof StatefulSet) {
        client.apps().statefulSets().create((StatefulSet) spec);
      } else if (spec instanceof Service) {
        client.services().create((Service) spec);
      } else if (spec instanceof Job) {
        client.batch().jobs().create((Job) spec);
      } else if (spec instanceof IstioResource) {
        istioClient.inNamespace(config.getNamespace()).registerCustomResource((IstioResource) spec);
      } else {
        throw new ISE("Unknown spec for deployment WTF!!!" + spec.getKind());
      }
    }
  }

  private void delete(List<HasMetadata> hasMetadata)
  {
    for (HasMetadata spec : hasMetadata) {
      if (spec instanceof StatefulSet) {
        client.apps().statefulSets().delete((StatefulSet) spec);
      } else if (spec instanceof Service) {
        client.services().delete((Service) spec);
      } else if (spec instanceof Job) {
        client.batch().jobs().delete((Job) spec);
      } else if (spec instanceof IstioResource) {
        // TODO: cleaning up istio throws exception, check the version number.
        //istioClient.inNamespace(config.getNamespace()).unregisterCustomResource((IstioResource) spec);
      } else {
        throw new ISE("Unknown spec for deployment WTF!!!" + spec.getKind());
      }
    }
  }

  @Override
  public void shutdown(String taskid, String reason)
  {
    synchronized (statusLock) {
      KubernetesTaskRunnerWorkItem remove = tasks.remove(taskid);
      delete(remove.getCharts());
      remove.setResult(TaskStatus.failure(taskid));
      remove.setState(KubernetesTaskRunnerWorkItem.State.COMPLETE);
    }
  }

  @Override
  public void stop()
  {

  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == KubernetesTaskRunnerWorkItem.State.RUNNING)
                .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == KubernetesTaskRunnerWorkItem.State.PENDING)
                .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.copyOf(tasks.values());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset)
  {
    if (!tasks.containsKey(taskid)) {
      // Worker is not running this task, it might be available in deep storage
      return Optional.absent();
    }
    Pod pod = client.pods()
                    .inNamespace(config.getNamespace())
                    .withLabel("job-name=" + sanitizeName(taskid))
                    .list()
                    .getItems()
                    .get(0);

    String log = client.pods()
                       .inNamespace(config.getNamespace())
                       .withName(pod.getMetadata().getName())
                       .inContainer("druid")
                       .getLog();
    return Optional.of(new ByteSource()
    {
      @Override
      public InputStream openStream()
      {
        return IOUtils.toInputStream(log, Charsets.UTF_8);
      }
    });
  }
}

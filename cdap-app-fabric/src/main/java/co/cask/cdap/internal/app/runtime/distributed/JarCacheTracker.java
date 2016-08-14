/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.proto.ProgramType;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimization for CDAP-7021.
 *
 * We cache appmaster and container jars created by twill the first time a program
 * of that type is run. However, if 100 programs are started at the same time, all 100 will still create those
 * jars since the cached jars won't be available at run start. So as an optimization, if a program is
 * launching and its cached jars are not available, and its not the first program of its type to be launched,
 * and its launching within 2 minutes of the first run, wait for the cache jars to become available.
 * without this, we might actually miss schedule runs due to jar building taking too much time and resources.
 */
public class JarCacheTracker {
  public static final JarCacheTracker INSTANCE = new JarCacheTracker();
  private static final Logger LOG = LoggerFactory.getLogger(JarCacheTracker.class);
  private final Map<ProgramType, AtomicLong> firstLaunchTimes;

  private JarCacheTracker() {
    firstLaunchTimes = new ConcurrentHashMap<>();
    for (ProgramType programType : ProgramType.values()) {
      firstLaunchTimes.put(programType, new AtomicLong(-1L));
    }
  }

  public void registerLaunch(File cacheDir, ProgramType programType) {
    // if this launch is the first of its type, the set will go through and we skip waiting for the cache
    long launchTime = System.currentTimeMillis();
    AtomicLong firstLaunchTime = firstLaunchTimes.get(programType);
    if (!firstLaunchTime.compareAndSet(-1L, launchTime)) {
      File cacheDoneFile = new File(cacheDir, "container.jar.done").getAbsoluteFile();
      long threshold = TimeUnit.MILLISECONDS.convert(90, TimeUnit.SECONDS);
      LOG.debug("Waiting for cache done file {} to exist. Will wait a max of {} ms.",
                cacheDoneFile.getAbsolutePath(), threshold);
      // otherwise, wait for 90 seconds past the first launch, or for the cache to be ready.
      boolean cacheExists = cacheDoneFile.exists();
      while ((System.currentTimeMillis() - firstLaunchTime.get() < threshold) && !cacheExists) {
        try {
          TimeUnit.SECONDS.sleep(5L);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for first twill cache jars for program type {}." +
                     " Continuing with launch.", programType, e);
          break;
        }
        cacheExists = cacheDoneFile.exists();
      }
      if (!cacheExists) {
        LOG.info("Cache jars for program type {} don't exist, continuing on.", programType);
      } else {
        LOG.debug("Found cache jars for program type {}, continuing on.", programType);
      }
    } else {
      LOG.debug("First program of type {} launched at time {}", programType, launchTime);
    }
  }

}

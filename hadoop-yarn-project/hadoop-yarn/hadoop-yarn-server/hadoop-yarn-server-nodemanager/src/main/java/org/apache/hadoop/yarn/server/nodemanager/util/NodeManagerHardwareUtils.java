/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.Map;

/**
 * Helper class to determine hardware related characteristics such as the
 * number of processors and the amount of memory on the node.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NodeManagerHardwareUtils {

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeManagerHardwareUtils.class);

  private static boolean isHardwareDetectionEnabled(Configuration conf) {
    return conf.getBoolean(
        YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        YarnConfiguration.DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION);
  }

  /**
   *
   * Returns the number of CPUs on the node. This value depends on the
   * configuration setting which decides whether to count logical processors
   * (such as hyperthreads) as cores or not.
   *
   * @param conf
   *          - Configuration object
   * @return Number of CPUs
   */
  public static int getNodeCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
  }

  /**
   *
   * Returns the number of CPUs on the node. This value depends on the
   * configuration setting which decides whether to count logical processors
   * (such as hyperthreads) as cores or not.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - Configuration object
   * @return Number of CPU cores on the node.
   */
  public static int getNodeCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = plugin.getNumProcessors();
    boolean countLogicalCores =
        conf.getBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
          YarnConfiguration.DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES);
    if (!countLogicalCores) {
      numProcessors = plugin.getNumCores();
    }
    return numProcessors;
  }

  /**
   *
   * Returns the fraction of CPUs that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param conf
   *          - Configuration object
   * @return Fraction of CPUs to be used for YARN containers
   */
  public static float getContainersCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
  }

  /**
   *
   * Returns the fraction of CPUs that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - Configuration object
   * @return Fraction of CPUs to be used for YARN containers
   */
  public static float getContainersCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = getNodeCPUs(plugin, conf);
    int nodeCpuPercentage = getNodeCpuPercentage(conf);

    return (nodeCpuPercentage * numProcessors) / 100.0f;
  }

  /**
   * Gets the percentage of physical CPU that is configured for YARN containers.
   * This is percent {@literal >} 0 and {@literal <=} 100 based on
   * {@link YarnConfiguration#NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT}
   * @param conf Configuration object
   * @return percent {@literal >} 0 and {@literal <=} 100
   */
  public static int getNodeCpuPercentage(Configuration conf) {
    int nodeCpuPercentage =
        Math.min(conf.getInt(
          YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
          YarnConfiguration.DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT),
          100);
    nodeCpuPercentage = Math.max(0, nodeCpuPercentage);

    if (nodeCpuPercentage == 0) {
      String message =
          "Illegal value for "
              + YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
              + ". Value cannot be less than or equal to 0.";
      throw new IllegalArgumentException(message);
    }
    return nodeCpuPercentage;
  }

  private static int getConfiguredVCores(Configuration conf) {
    // yarn.nodemanager.resource.cpu-vcores : 默认 8
    int cores = conf.getInt(YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    if (cores == -1) {
      cores = YarnConfiguration.DEFAULT_NM_VCORES;
    }
    return cores;
  }

  /**
   * Function to return the number of vcores on the system that can be used for
   * YARN containers. If a number is specified in the configuration file, then
   * that number is returned. If nothing is specified - 1. If the OS is an
   * "unknown" OS(one for which we don't have ResourceCalculatorPlugin
   * implemented), return the default NodeManager cores. 2. If the config
   * variable yarn.nodemanager.cpu.use_logical_processors is set to true, it
   * returns the logical processor count(count hyperthreads as cores), else it
   * returns the physical cores count.
   *
   * @param conf
   *          - the configuration for the NodeManager
   * @return the number of cores to be used for YARN containers
   *
   */
  public static int getVCores(Configuration conf) {


    // yarn.nodemanager.resource.detect-hardware-capabilities : false
    if (!isHardwareDetectionEnabled(conf)) {
      return getConfiguredVCores(conf);
    }
    // is this os for which we can determine cores?
    ResourceCalculatorPlugin plugin = ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  /**
   * Function to return the number of vcores on the system that can be used for
   * YARN containers. If a number is specified in the configuration file, then
   * that number is returned. If nothing is specified - 1. If the OS is an
   * "unknown" OS(one for which we don't have ResourceCalculatorPlugin
   * implemented), return the default NodeManager cores. 2. If the config
   * variable yarn.nodemanager.cpu.use_logical_processors is set to true, it
   * returns the logical processor count(count hyperthreads as cores), else it
   * returns the physical cores count.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - the configuration for the NodeManager
   * @return the number of cores to be used for YARN containers
   *
   */
  public static int getVCores(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  // 通过配置获取 cpu : 物理cpu *  系数(默认1)
  private static int getVCoresInternal(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    String message;

    // 获取配置文件中的参数 :  yarn.nodemanager.resource.cpu-vcores : -1

    int cores = conf.getInt(YarnConfiguration.NM_VCORES, -1);
    if (cores == -1) {

      // 如果配置文件中的参数没有设置的话,开始进入自动识别模式.

      // 获取通过实现类 获取物理机cpu数量
      float physicalCores =  NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);


      // 一个物理cpu可以虚拟成几个cpu ?  默认值 1
      // yarn.nodemanager.resource.pcores-vcores-multiplier : default 1
      float multiplier = conf.getFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER,  YarnConfiguration.DEFAULT_NM_PCORES_VCORES_MULTIPLIER);

      if (multiplier > 0) {

        //  物理机cpu * 虚拟cpu的权重
        float tmp = physicalCores * multiplier;

        if (tmp > 0 && tmp < 1) {
          // on a single core machine - tmp can be between 0 and 1
          cores = 1;
        } else {
          // 处理一下, 四舍五入获取一个 int类型的数值用于标识cpu
          cores = Math.round(tmp);
        }
      } else {
        message = "Illegal value for "
            + YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER
            + ". Value must be greater than 0.";
        throw new IllegalArgumentException(message);
      }
    }
    if(cores <= 0) {
      message = "Illegal value for " + YarnConfiguration.NM_VCORES
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }
    // 返回cpu的数据量
    return cores;
  }

  private static long getConfiguredMemoryMB(Configuration conf) {

    // yarn.nodemanager.resource.memory-mb :  8g
    long memoryMb = conf.getLong(YarnConfiguration.NM_PMEM_MB,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    if (memoryMb == -1) {
      memoryMb = YarnConfiguration.DEFAULT_NM_PMEM_MB;
    }
    return memoryMb;
  }

  /**
   * Function to return how much memory we should set aside for YARN containers.
   * If a number is specified in the configuration file, then that number is
   * returned. If nothing is specified - 1. If the OS is an "unknown" OS(one for
   * which we don't have ResourceCalculatorPlugin implemented), return the
   * default NodeManager physical memory. 2. If the OS has a
   * ResourceCalculatorPlugin implemented, the calculation is 0.8 * (RAM - 2 *
   * JVM-memory) i.e. use 80% of the memory after accounting for memory used by
   * the DataNode and the NodeManager. If the number is less than 1GB, log a
   * warning message.
   *
   * @param conf
   *          - the configuration for the NodeManager
   * @return the amount of memory that will be used for YARN containers in MB.
   */
  public static long getContainerMemoryMB(Configuration conf) {
    // 是否自动检测节点的CPU和内存
    // yarn.nodemanager.resource.detect-hardware-capabilities : false
    if (!isHardwareDetectionEnabled(conf)) {
      // 获取配置: yarn.nodemanager.resource.memory-mb :  default  8g
      return getConfiguredMemoryMB(conf);
    }
    ResourceCalculatorPlugin plugin =  ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  /**
   * Function to return how much memory we should set aside for YARN containers.
   * If a number is specified in the configuration file, then that number is
   * returned. If nothing is specified - 1. If the OS is an "unknown" OS(one for
   * which we don't have ResourceCalculatorPlugin implemented), return the
   * default NodeManager physical memory. 2. If the OS has a
   * ResourceCalculatorPlugin implemented, the calculation is 0.8 * (RAM - 2 *
   * JVM-memory) i.e. use 80% of the memory after accounting for memory used by
   * the DataNode and the NodeManager. If the number is less than 1GB, log a
   * warning message.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - the configuration for the NodeManager
   * @return the amount of memory that will be used for YARN containers in MB.
   */
  public static long getContainerMemoryMB(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  private static long getContainerMemoryMBInternal(ResourceCalculatorPlugin plugin,  Configuration conf) {
    // 首先验证配置文件中是否已经设置, 如果设置了使用配置中的资源, 如果没有配置, 才会自动计算
    // yarn.nodemanager.resource.memory-mb : -1
    long memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB, -1);

    if (memoryMb == -1) {
      // 配置文件中没有配置yarn.nodemanager.resource.memory-mb, 开启自动配置模式

      // 计算物理资源 , 这里返回的单位是 bytes , 所以要换成MB
      long physicalMemoryMB = (plugin.getPhysicalMemorySize() / (1024 * 1024));

      // 获取jvm 可以使用的最大内存.
      long hadoopHeapSizeMB = (Runtime.getRuntime().maxMemory() / (1024 * 1024));

      // 容器可以使用的物理内存 = 0.8 *  (  物理内存 - 2 * JVM内存 )
      long containerPhysicalMemoryMB = (long) (0.8f * (physicalMemoryMB - (2 * hadoopHeapSizeMB)));

      // 系统预留的内存 : yarn.nodemanager.resource.system-reserved-memory-mb 默认值 -1
      long reservedMemoryMB = conf.getInt(YarnConfiguration.NM_SYSTEM_RESERVED_PMEM_MB, -1);

      if (reservedMemoryMB != -1) {
        //实际可以使用的内存为  可用的物理内存 - 系统预留内存
        containerPhysicalMemoryMB = physicalMemoryMB - reservedMemoryMB;
      }

      if (containerPhysicalMemoryMB <= 0) {
        LOG.error("Calculated memory for YARN containers is too low."
            + " Node memory is " + physicalMemoryMB
            + " MB, system reserved memory is " + reservedMemoryMB + " MB.");
      }
      // 做了一个参数验证, 内存不能为负数...
      containerPhysicalMemoryMB = Math.max(containerPhysicalMemoryMB, 0);
      memoryMb = containerPhysicalMemoryMB;
    }
    if(memoryMb <= 0) {
      String message = "Illegal value for " + YarnConfiguration.NM_PMEM_MB
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }
    return memoryMb;
  }

  /**
   * Get the resources for the node.
   * @param configuration configuration file
   * @return the resources for the node
   */
  public static Resource getNodeResources(Configuration configuration) {
    Configuration conf = new Configuration(configuration);
    // memory-mb
    String memory = ResourceInformation.MEMORY_MB.getName();
    // vcores
    String vcores = ResourceInformation.VCORES.getName();

    // LightWeightResource
    Resource ret = Resource.newInstance(0, 0);

    // vcores -> {ResourceInformation@3257} "name: vcores, units: , type: COUNTABLE, value: 0, minimum allocation: 1, maximum allocation: 4"
    // memory-mb -> {ResourceInformation@3256} "name: memory-mb, units: Mi, type: COUNTABLE, value: 0, minimum allocation: 1024, maximum allocation: 8192"
    Map<String, ResourceInformation> resourceInformation = ResourceUtils.getNodeResourceInformation(conf);


    for (Map.Entry<String, ResourceInformation> entry : resourceInformation .entrySet()) {

      ret.setResourceInformation(entry.getKey(), entry.getValue());

      LOG.info("Setting key " + entry.getKey() + " to " + entry.getValue());
    }

    // 处理内存
    if (resourceInformation.containsKey(memory)) {

      Long value = resourceInformation.get(memory).getValue();

      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource memory is more than the maximum for an integer.");
      }

      ResourceInformation memResInfo = resourceInformation.get(memory);

      if(memResInfo.getValue() == 0) {
        //处理 Memory
        ret.setMemorySize(getContainerMemoryMB(conf));

        LOG.debug("Set memory to " + ret.getMemorySize());
      }
    }

    // 处理cpu
    if (resourceInformation.containsKey(vcores)) {


      Long value = resourceInformation.get(vcores).getValue();


      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource vcores is more than the maximum for an integer.");
      }

      // name: vcores, units: , type: COUNTABLE, value: 0, minimum allocation: 1, maximum allocation: 4
      ResourceInformation vcoresResInfo = resourceInformation.get(vcores);


      if(vcoresResInfo.getValue() == 0) {
        ret.setVirtualCores(getVCores(conf));
        LOG.debug("Set vcores to " + ret.getVirtualCores());
      }
    }

    LOG.info("Node resource information map is " + ret);
    return ret;
  }
}

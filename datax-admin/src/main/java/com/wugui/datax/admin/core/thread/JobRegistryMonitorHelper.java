package com.wugui.datax.admin.core.thread;

import com.wugui.datatx.core.enums.RegistryConfig;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.entity.JobGroup;
import com.wugui.datax.admin.entity.JobRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * job registry instance
 * 流程：
 * 1.每30s 循环一次(所以执行器运行后，调度中心最慢将30s才会把它加入到执行器组)
 * 查询出 执行器组列表(自动注册到 调度中心的执行器 会通过 appname 匹配到任务组，
 * 此定时任务将匹配的执行器的IP 挂载到 这个执行器组，相当于一个nginx 可以路由到多个 tomcat)
 *
 * 2.xxl_job_registry 是执行器运行后 注册到调度中心的记录表，
 * 记录执行器的相关信息，这里通过扫描 xxl_job_registry 的更新时间
 * 确定执行器是否还存活(执行器会定时请求 调度中心的 /api/registry 接口 用户注册活更新心跳时间)，
 * 超过 90s 没有更新的 执行器将被移除
 *
 * 及时新增30s内新注册的执行器(findall)，清除90s内未再次注册的执行器（默认心跳保活时间30s）
 *
 * @author xuxueli 2016-10-02 19:10:24
 */
public class JobRegistryMonitorHelper {
	private static Logger logger = LoggerFactory.getLogger(JobRegistryMonitorHelper.class);

	private static JobRegistryMonitorHelper instance = new JobRegistryMonitorHelper();
	public static JobRegistryMonitorHelper getInstance(){
		return instance;
	}

	private Thread registryThread;
	private volatile boolean toStop = false;
	public void start(){
		// 1。删除过期的 执行器 2. 绑定 appname 和 多个执行器
		registryThread = new Thread(() -> {
			while (!toStop) {
				try {
					// 查询出 执行器组列表(自动注册到 调度中心的执行器 会通过 appname 匹配到任务组，
					// 此定时任务将匹配的执行器的IP 挂载到 这个执行器组，相当于一个nginx 可以路由到多个 tomcat)
//					findDead 的条件是 update_time < nowTime - timeout【90秒】，即上次更新时间为 90 秒之前，而执行器端的心跳周期是 30 秒，超过这个时间，视为失联。
//
//					findAll 则相反，update_time > nowTime - timeout【90秒】。
					List<JobGroup> groupList = JobAdminConfig.getAdminConfig().getJobGroupMapper().findByAddressType(0);
					if (groupList!=null && !groupList.isEmpty()) {

						// // findDead 查找 90 秒以内未更新过的执行器，视为无效记录，并删除；
						List<Integer> ids = JobAdminConfig.getAdminConfig().getJobRegistryMapper().findDead(RegistryConfig.DEAD_TIMEOUT, new Date());
						if (ids!=null && ids.size()>0) {
							//移除dead executor
							JobAdminConfig.getAdminConfig().getJobRegistryMapper().removeDead(ids);
						}

						// 新的在线地址
						HashMap<String, List<String>> appAddressMap = new HashMap<>();
						//findAll查询有效执行器，以执行器的 app_name 归并到 Group 表中对应集群的 address_list 字段。
						List<JobRegistry> list = JobAdminConfig.getAdminConfig().getJobRegistryMapper().findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
						if (list != null) {
							// 扫描 执行器 list，通过 HashMap 的特性把 相同 appname 的执行器整合到一条数据
							for (JobRegistry item: list) {
								//更新 执行器集群下 每个组 的 list
								if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
									String appName = item.getRegistryKey();
									List<String> registryList = appAddressMap.get(appName);
									if (registryList == null) {
										registryList = new ArrayList<>();
									}

									if (!registryList.contains(item.getRegistryValue())) {
										registryList.add(item.getRegistryValue());
									}
									appAddressMap.put(appName, registryList);
								}
							}
						}

						// 循环 执行器组，通过appname 匹配 上面处理好的 map 集合，把相同 appname 的执行器地址通过逗号分隔的方式写入 执行器组
						for (JobGroup group: groupList) {
							List<String> registryList = appAddressMap.get(group.getAppName());
							String addressListStr = null;
							if (registryList!=null && !registryList.isEmpty()) {
								Collections.sort(registryList);
								addressListStr = "";
								for (String item:registryList) {
									addressListStr += item + ",";
								}
								addressListStr = addressListStr.substring(0, addressListStr.length()-1);
							}
							group.setAddressList(addressListStr);
							//修改 xxl_job_group 一个 appname 绑定 多个地址
							JobAdminConfig.getAdminConfig().getJobGroupMapper().update(group);
						}
					}
				} catch (Exception e) {
					if (!toStop) {
						logger.error(">>>>>>>>>>> datax-web, job registry monitor thread error:{}", e);
					}
				}
				try {
					//三十秒刷新一次
					TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
				} catch (InterruptedException e) {
					if (!toStop) {
						logger.error(">>>>>>>>>>> datax-web, job registry monitor thread error:{}", e);
					}
				}
			}
			logger.info(">>>>>>>>>>> datax-web, job registry monitor thread stop");
		});
		//设置为守护线程
		registryThread.setDaemon(true);
		registryThread.setName("datax-web, admin JobRegistryMonitorHelper");
		registryThread.start();
	}

	public void toStop(){
		toStop = true;
		// interrupt and wait
		registryThread.interrupt();
		try {
			registryThread.join();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

}

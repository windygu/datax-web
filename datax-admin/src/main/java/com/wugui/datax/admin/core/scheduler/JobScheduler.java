package com.wugui.datax.admin.core.scheduler;

import com.wugui.datatx.core.biz.ExecutorBiz;
import com.wugui.datatx.core.enums.ExecutorBlockStrategyEnum;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.thread.*;
import com.wugui.datax.admin.core.util.I18nUtil;
import com.wugui.datax.rpc.remoting.invoker.call.CallType;
import com.wugui.datax.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.wugui.datax.rpc.remoting.invoker.route.LoadBalance;
import com.wugui.datax.rpc.remoting.net.impl.netty_http.client.NettyHttpClient;
import com.wugui.datax.rpc.serialize.impl.HessianSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 * XxlJobScheduler 是 xxl-job-admin 非常核心的一个类。各功能的启动【入口】
 * 1.JobRegistryMonitorHelper-启动一个线程，定时扫描过期的执行器、扫描执行器绑定到 对应的 appname 上。
 * 2.JobFailMonitorHelper-启动一个线程，定时扫描需要重试的任务、如果设置了告警 那么触发消息通知。
 * 3.JobLosedMonitorHelper-启动一个线程，定时扫描将任务处理结果丢失且超过10分钟，执行器没有了心跳💗的调度记录 主动处理为失败。
 * 4.JobTriggerPoolHelper-初始化 一个快速处理的线程池和一个慢处理的线程池 分别执行时间消耗不一样的任务，加快任务执行性能，比较好的一个设计。
 * 5.JobLogReportHelper-启动一个线程，定时扫描任务执行和日志信息统计称报告信息，用于展示。
 * 6.JobScheduleHelper-启动一个线程，定时扫描5s中内需要执行的任务，触发任务处理。
 */

public class JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);


    public void init() throws Exception {
        // 初始化 执行器阻塞策略 的国际化
        initI18n();

        // 删除过期执行器&更新新增执行器
        JobRegistryMonitorHelper.getInstance().start();

        // 1.重试需要重试的任务 2.告警设置了告警的任务
        JobFailMonitorHelper.getInstance().start();

        // 初始化了 一个 快速 和 一个慢 的 线程池，根据历史任务执行时间划分
        JobTriggerPoolHelper.toStart();

        // 运行报告统计
        JobLogReportHelper.getInstance().start();

        // 用于任务触发（重点）。定时扫描 需要执行的任务，并计算下一次执行的时间
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init datax-web admin success.");
    }


    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

        // admin monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryMonitorHelper.getInstance().toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address == null || address.trim().length() == 0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        XxlRpcReferenceBean referenceBean = new XxlRpcReferenceBean();
        referenceBean.setClient(NettyHttpClient.class);
        referenceBean.setSerializer(HessianSerializer.class);
        referenceBean.setCallType(CallType.SYNC);
        referenceBean.setLoadBalance(LoadBalance.ROUND);
        referenceBean.setIface(ExecutorBiz.class);
        referenceBean.setVersion(null);
        referenceBean.setTimeout(3000);
        referenceBean.setAddress(address);
        referenceBean.setAccessToken(JobAdminConfig.getAdminConfig().getAccessToken());
        referenceBean.setInvokeCallback(null);
        referenceBean.setInvokerFactory(null);

        executorBiz = (ExecutorBiz) referenceBean.getObject();

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}

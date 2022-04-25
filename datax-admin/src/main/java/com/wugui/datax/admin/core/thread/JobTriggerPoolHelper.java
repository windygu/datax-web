package com.wugui.datax.admin.core.thread;

import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.trigger.TriggerTypeEnum;
import com.wugui.datax.admin.core.trigger.JobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * JobTriggerPoolHelper 用于初始化一个 快速线程池和一个慢线程池。
 *
 * 流程：
 * 1.初始化一个快速线程池执行器
 * 2.初始化一个慢线程池执行器
 * 快线程池用于处理时间短的任务，慢线程池用于处理时间长的任务
 * 当采用集群部署时，JobTriggerPoolHelper类在程序运行中会有哪些问题？提示：volatile，AtomicInteger，ConcurrentMap。
 * 主要职能：线程池异步触发任务
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        // 这里是定义了2个线程池，看名字是快和慢的trigger调度线程池。是根据job的历史执行情况，将不同的任务放入不同的线程池，供后续执行
        // 这里只是定义了线程池，在哪里将job工作线程放入和取出去调用executor执行器的呢？重点在JobScheduleHelper中的scheduleThread和ringThread。
        //此处初始化一个快速线程池执行器
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                //声明一个队列
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "datax-web, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "datax-web, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }


    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> datax-web trigger thread pool shutdown success.");
    }


    // job timeout count
    //minTim 变量和jobTimeoutCountMap变量都使用volatile关键字修饰，但在集群环境下此处的计数就会不准确，
    // 当然对任务的执行并无影响，只是进来慢线程池的时间可能会延迟一些概率也就降低了
    //return：the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
    private volatile long minTim = System.currentTimeMillis() / 60000;     // 当前时间，按分钟表示
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * add trigger
     * 流程：
     * 1.根据前面任务的执行情况，判断走 快速线程还是满线程
     * 2.做任务调用，通过http 请求执行器
     * 3.统计执行时间，1分钟内 累计执行超过500ms 的次数超过10次，那么下一次此任务执行将放入慢任务池
     */
    public void addTrigger(final int jobId, final TriggerTypeEnum triggerType, final int failRetryCount, final String executorShardingParam, final String executorParam) {

        // choose thread pool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        //一分钟某job超时十次则走慢线程
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }
        // trigger
        triggerPool_.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                // do trigger
                JobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                // 每新的一分钟清空map，重新判断快慢
                long minTim_now = System.currentTimeMillis() / 60000;
                if (minTim != minTim_now) {
                    minTim = minTim_now;
                    jobTimeoutCountMap.clear();
                }
                // trigger前后执行超过500毫秒，则在超时map里对应id，增加1
                long cost = System.currentTimeMillis() - start;
                if (cost > 500) {       // ob-timeout threshold 500ms
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }


    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }

    public static void toStop() {
        helper.stop();
    }

    /**
     * @param jobId
     * @param triggerType
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: 覆盖job param
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam);
    }

}

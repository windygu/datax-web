package com.wugui.datax.admin.core.thread;

import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.cron.CronExpression;
import com.wugui.datax.admin.core.trigger.TriggerTypeEnum;
import com.wugui.datax.admin.entity.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 * 流程：
 * 1.启动一个线程，随机休眠 4-5s 防止 多个线程同时启动竞争（猜想）
 * 2.计算出一次性读取的任务个数，给出的 计算公式为：pre-read count: treadpool-size * trigger-qps，
 * (1s=1000ms；每个任务花费50ms，qps = 1000/50 = 20)，一秒钟可以处理20个任务，默认值为 6000 个
 * 3.查询 xxl_job_lock 表 只有一条数据 用做读取任务的锁，每次调度者读取任务之前要先获得这把锁，通过 mysql for update 悲观锁的方式实现，
 * 保证任务不会被重复执行。
 * 4.查询任务状态为“运行中” 且下次执行时间在 5s中以内的任务。
 * 5.循环任务判断，这里分三种情况：
 *
 * 任务的执行时间已经过期了 5s，那么直接忽略不执行，并用当前时间计算下次执行的时间。
 * 任务的执行时间已经过期了 但没有大于 5s，可能是 2s、3s、4s，马上触发一次执行，并计算下一次执行时间， 判断计算出来的下一次执行时间是否在 5s以内，如果是 那么放入时间轮，并计算下一次执行的时间。
 * 还没到任务执行时间的任务 放入时间轮，并计算下一次执行的时间。
 */
public class JobScheduleHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobScheduleHelper instance = new JobScheduleHelper();

    public static JobScheduleHelper getInstance() {
        return instance;
    }

    public static final long PRE_READ_MS = 5000;    // 五秒

    private Thread scheduleThread;
    private Thread ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop = false;

    //时间轮本质是个concurrentHashMap，使用list解决hash冲突，六十秒一圈
    //https://img-blog.csdnimg.cn/img_convert/c24e46a6fa64fe2858a071ebffbef8d3.png#pic_center
    private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    public void start() {

        // schedule thread
        scheduleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //随机休眠 4-5s 防止 多个线程同时启动竞争（猜想）
                    TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!scheduleThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
                logger.info(">>>>>>>>> init datax-web admin scheduler success.");

                // pre-read count: treadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
                // 一次性预读取jobInfo的最大数量，2个pool的和 * 20
                int preReadCount = (JobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + JobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

                while (!scheduleThreadToStop) {

                    // Scan Job
                    long start = System.currentTimeMillis();

                    Connection conn = null;
                    Boolean connAutoCommit = null;
                    PreparedStatement preparedStatement = null;

                    boolean preReadSuc = true;
                    try {

                        conn = JobAdminConfig.getAdminConfig().getDataSource().getConnection();
                        connAutoCommit = conn.getAutoCommit();
                        conn.setAutoCommit(false);

                        // 悲观锁，获取到锁的机器开始执行jobInfo的读取以及后续调度工作，《《admin集群》》中没有获取到锁的admin继续等待下一次预读取
                        //当执行select status from t_goods where id=1 for update;后。在另外的事务中如果再次执行select status from t_goods where id=1 for update;则第二个事务会一直等待第一个事务的提交，此时第二个查询处于阻塞的状态
                        preparedStatement = conn.prepareStatement("select * from job_lock where lock_name = 'schedule_lock' for update");
                        preparedStatement.execute();

                        // txx start
                        //
                        //                        // 1、pre read，这里就用到了preReadCount和时间PRE_READ_MS（5000），就是读取5s内将要执行的jobInfo
                        //                        long nowTime = System.currentTimeMillis();
                        //                        List<JobInfo> scheduleList = JobAdminConfig.getAdminConfig().getJobInfoMapper().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        //                        if (scheduleList != null && scheduleList.size() > 0) {
                        //                            // 2、push time-ring
                        //                            for (JobInfo jobInfo : scheduleList) {
                        //
                        //                                // time-ring jump
                        //                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                        //                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                        //                                    //如果当前时间已经超过原先job的触发时间5s了，那么本次就不触发了，设置下一次时间。
                        //                                    logger.warn(">>>>>>>>>>> datax-web, schedule misfire, jobId = " + jobInfo.getId());
                        //
                        //                                    // fresh next
                        //                                    refreshNextValidTime(jobInfo, new Date());
                        //
                        //                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                        //                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                        //                                    //超时小于5秒，给予一定容忍度，立刻触发并刷新
                        //                                    // 1、trigger
                        //                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null);
                        //                                    logger.debug(">>>>>>>>>>> datax-web, schedule push trigger : jobId = " + jobInfo.getId());
                        //
                        //                                    // 2、fresh next
                        //                                    refreshNextValidTime(jobInfo, new Date());
                        //
                        //                                    // next-trigger-time in 5s, pre-read again
                        //                                    if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {
                        //
                        //                                        // 1、make ring second
                        //                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                        //
                        //                                        // 2、push time ring
                        //                                        pushTimeRing(ringSecond, jobInfo.getId());
                        //
                        //                                        // 3、fresh next
                        //                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                        //
                        //                                    }
                        //
                        //                                } else {
                        //                                    //针对还未超时的任务，正常放入时间轮
                        //                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time
                        //
                        //                                    // 1、make ring second
                        //                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);
                        //
                        //                                    // 2、push time ring
                        //                                    pushTimeRing(ringSecond, jobInfo.getId());
                        //
                        //                                    // 3、fresh next
                        //                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                        //
                        //                                }
                        //
                        //                            }
                        //
                        //                            // 3、update trigger info
                        //                            for (JobInfo jobInfo : scheduleList) {
                        //                                //更新1.上次触发时间 2.下次xx 3.触发状态
                        //                                JobAdminConfig.getAdminConfig().getJobInfoMapper().scheduleUpdate(jobInfo);
                        //                            }
                        //
                        //                        } else {
                        //                            //预读失败
                        //                            preReadSuc = false;
                        //                        } start

                        // 1、pre read，这里就用到了preReadCount和时间PRE_READ_MS（5000），就是读取5s内将要执行的jobInfo
                        long nowTime = System.currentTimeMillis();
                        List<JobInfo> scheduleList = JobAdminConfig.getAdminConfig().getJobInfoMapper().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                        if (scheduleList != null && scheduleList.size() > 0) {
                            // 2、push time-ring
                            for (JobInfo jobInfo : scheduleList) {

                                // time-ring jump
                                if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
                                    // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                    //如果当前时间已经超过原先job的触发时间5s了，那么本次就不触发了，设置下一次时间。
                                    logger.warn(">>>>>>>>>>> datax-web, schedule misfire, jobId = " + jobInfo.getId());

                                    // fresh next
                                    refreshNextValidTime(jobInfo, new Date());

                                } else if (nowTime > jobInfo.getTriggerNextTime()) {
                                    // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                                    //超时小于5秒，给予一定容忍度，立刻触发并刷新
                                    // 1、trigger，扔给pool执行任务
                                    JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null);
                                    logger.debug(">>>>>>>>>>> datax-web, schedule push trigger : jobId = " + jobInfo.getId());

                                    // 2、fresh next
                                    refreshNextValidTime(jobInfo, new Date());

                                    // next-trigger-time in 5s, pre-read again
                                    if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {

                                        // 1、make ring second
                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                        // 2、push time ring
                                        pushTimeRing(ringSecond, jobInfo.getId());

                                        // 3、fresh next
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                    }

                                } else {
                                    //针对还未超时的任务，正常放入时间轮
                                    // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time

                                    // 1、make ring second
                                    int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                    // 2、push time ring
                                    pushTimeRing(ringSecond, jobInfo.getId());

                                    // 3、fresh next
                                    refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

                                }

                            }

                            // 3、update trigger info
                            for (JobInfo jobInfo : scheduleList) {
                                //更新1.上次触发时间 2.下次xx 3.触发状态
                                JobAdminConfig.getAdminConfig().getJobInfoMapper().scheduleUpdate(jobInfo);
                            }

                        } else {
                            //预读失败
                            preReadSuc = false;
                        }

                        // tx stop
                    } catch (Exception e) {
                        if (!scheduleThreadToStop) {
                            logger.error(">>>>>>>>>>> datax-web, JobScheduleHelper#scheduleThread error:{}", e);
                        }
                    } finally {

                        // commit
                        if (conn != null) {
                            try {
                                conn.commit();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.setAutoCommit(connAutoCommit);
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }

                        // close PreparedStatement
                        if (null != preparedStatement) {
                            try {
                                preparedStatement.close();
                            } catch (SQLException e) {
                                if (!scheduleThreadToStop) {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                    long cost = System.currentTimeMillis() - start;


                    // 如果用时小于1秒
                    if (cost < 1000) {  // scan-overtime, not wait
                        try {
                            // pre-read period: success 则 scan each second; fail 则 skip this period;
                            TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                        } catch (InterruptedException e) {
                            if (!scheduleThreadToStop) {
                                logger.error(e.getMessage(), e);
                            }
                        }
                    }

                }

                logger.info(">>>>>>>>>>> datax-web, JobScheduleHelper#scheduleThread stop");
            }
        });
        scheduleThread.setDaemon(true);
        scheduleThread.setName("datax-web, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();


        // ring thread
        //ring thread读取ringData，也就是上面thread加入时间轮的数据
        ringThread = new Thread(() -> {

            // align second
            //随机睡小于一秒
            try {
                TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
            } catch (InterruptedException e) {
                if (!ringThreadToStop) {
                    logger.error(e.getMessage(), e);
                }
            }

            while (!ringThreadToStop) {

                try {
                    // second data
                    List<Integer> ringItemData = new ArrayList<>();
                    // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
                    //ring不需要自动转，比如添加到ring时的秒数为45秒，只会读取50秒内，也就是5秒内即将被执行
                    //的任务，那么3秒后要执行的就会去到48刻度。ringThread在48的时候取出刚才加的job
                    //ring内的刻度不是表示几秒后被执行，而是该秒时执行。 "45"代表该分钟45秒时执行，而不是加入ring时间的后45秒执行
                    int nowSecond = Calendar.getInstance().get(Calendar.SECOND);
                    //取当前秒数和前一秒（本应该在上一秒取出的）ring里的任务，防止以前的任务被遗漏
                    for (int i = 0; i < 2; i++) {
                        List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                        if (tmpData != null) {
                            ringItemData.addAll(tmpData);
                        }
                    }

                    // ring trigger
                    logger.debug(">>>>>>>>>>> datax-web, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData));
                    if (ringItemData.size() > 0) {
                        // do trigger
                        for (int jobId : ringItemData) {
                            // 扔给pool执行任务
                            JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null);
                        }
                        // clear
                        ringItemData.clear();
                    }
                } catch (Exception e) {
                    if (!ringThreadToStop) {
                        logger.error(">>>>>>>>>>> datax-web, JobScheduleHelper#ringThread error:{}", e);
                    }
                }

                // next second, align second
                try {
                    TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                } catch (InterruptedException e) {
                    if (!ringThreadToStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> datax-web, JobScheduleHelper#ringThread stop");
        });
        ringThread.setDaemon(true);
        ringThread.setName("datax-web, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    private void refreshNextValidTime(JobInfo jobInfo, Date fromTime) throws ParseException {
        Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(fromTime);
        if (nextValidTime != null) {
            jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
            jobInfo.setTriggerNextTime(nextValidTime.getTime());
        } else {
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
        }
    }

    private void pushTimeRing(int ringSecond, int jobId) {
        // push async ring
        List<Integer> ringItemData = ringData.get(ringSecond);
        if (ringItemData == null) {
            ringItemData = new ArrayList<Integer>();
            ringData.put(ringSecond, ringItemData);
        }
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> datax-web, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData));
    }

    public void toStop() {

        // 1、stop schedule
        scheduleThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);  // wait
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            scheduleThread.interrupt();
            try {
                scheduleThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty()) {
            for (int second : ringData.keySet()) {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && tmpData.size() > 0) {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData) {
            try {
                TimeUnit.SECONDS.sleep(8);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED) {
            // interrupt and wait
            ringThread.interrupt();
            try {
                ringThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> datax-web, JobScheduleHelper stop");
    }

}

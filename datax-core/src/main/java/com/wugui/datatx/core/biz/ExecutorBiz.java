package com.wugui.datatx.core.biz;

import com.wugui.datatx.core.biz.model.LogResult;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.biz.model.TriggerParam;

/**
 * Created by xuxueli on 17/3/1.
 * 用于向executor服务器提供请求，其实是client
 */
public interface ExecutorBiz {

    public static void main(String[] args) {
        System.out.println(ExecutorBiz.class.getName());
    }
    /**
     * 用于检查心跳；直接返回成功
     *
     * @return
     */
    ReturnT<String> beat();

    /**
     * 用于检查忙碌状态；忙碌中（执行任务中，或者队列中有数据）
     *
     * @param jobId
     * @return
     */
    ReturnT<String> idleBeat(int jobId);

    /**
     * 用于中断线程；
     *
     * @param jobId
     * @return
     */
    ReturnT<String> kill(int jobId);

    /**
     * 用于读取日志；
     *
     * @param logDateTim
     * @param logId
     * @param fromLineNum
     * @return
     */
    ReturnT<LogResult> log(long logDateTim, long logId, int fromLineNum);

    /**
     * 用于执行任务；
     *
     * @param triggerParam
     * @return
     */
    ReturnT<String> run(TriggerParam triggerParam);
}

package com.github.zjiajun.kafka.zk;

/**
 * @author zhujiajun
 * @since 2016/11/5
 */
public interface ElectionCallback {

    void becomeLeader(String leaderPath);

}

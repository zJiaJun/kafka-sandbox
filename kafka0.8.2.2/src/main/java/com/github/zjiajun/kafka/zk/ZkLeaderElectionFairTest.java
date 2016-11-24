package com.github.zjiajun.kafka.zk;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhujiajun
 * @since 2016/11/5
 */
public class ZkLeaderElectionFairTest {

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch mainLatch = new CountDownLatch(1);

        ThreadFactory threadFactory = new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "客户端机器-" + threadNumber.getAndIncrement());
                if (t.isDaemon()) t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                return t;
            }
        };

        ExecutorService executorService = new ThreadPoolExecutor(3,3,0,
                TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(10),threadFactory);

        ElectionCallback callback = new ElectionCallback() {

            @Override
            public void becomeLeader(String leaderPath) {
                System.err.format(Thread.currentThread().getName() + " 回调方法执行 : 节点 %s 成为领导者\n", leaderPath);
            }
        };

        for (int i =0;i< 3;i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    ZkLeaderElectionFair leaderElection = ZkLeaderElectionFairFactory.builder().connectString("127.0.0.1:2181").sessionTimeout(8000)
                            .rootPath("/leaderRoot").callback(callback).build();
                    boolean result = leaderElection.start(false);
                    System.out.println(Thread.currentThread().getName() + "选举领导是否成功 : " + result);
                }
            });
        }

        executorService.shutdown();
        mainLatch.await();
    }
}

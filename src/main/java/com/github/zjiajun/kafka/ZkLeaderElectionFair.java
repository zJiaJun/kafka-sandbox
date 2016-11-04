package com.github.zjiajun.kafka;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author zhujiajun
 * @since 2016/11/2
 *
 *  Zookeeper 原生api实现LeaderElection 公平模式
 *
 *  用Zookeeper原生提供的SDK实现“先到先得——公平模式”的Leader Election。即各参与方都注册ephemeral_sequential节点，ID较小者为leader
 *  每个竞选失败的参与方，只能watch前一个
 *  要能处理部分参与方等待过程中失败的情况
 *  实现2个选举方法，一个阻塞直到获得leadership，另一个竞选成功则返回true，否则返回失败，但要能支持回调
 */
public class ZkLeaderElectionFair {

    private ZooKeeper zooKeeper;
    private final String rootPath;
    private final CountDownLatch initZkLatch = new CountDownLatch(1);
    private final CountDownLatch leaderLatch = new CountDownLatch(1);

    public String getRootPath() {
        return rootPath;
    }

    public ZkLeaderElectionFair(String connectString, int sessionTimeout, String rootPath) {
        this.rootPath = rootPath;
        initZk(connectString,sessionTimeout);
        initRootPath();
    }

    private void initZk(String connectString, int sessionTimeout) {
        try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeout, event -> {
                switch (event.getState()) {
                    case SyncConnected:
                        System.err.println(event);
                        initZkLatch.countDown();
                        break;
                    case Disconnected:
                        System.err.println("Disconnected");
                        break;
                    default:
                        break;
                }
            });
            initZkLatch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initRootPath() {
        if (exists(rootPath, null)) {
            System.err.format("Path %s already exists\n", rootPath);
        } else {
            String path = createNode(rootPath, null, CreateMode.PERSISTENT);
            System.err.format("Create Root Path %s success\n", path);
        }
    }

    private boolean exists(String path, Watcher watcher) {
        try {
            Stat stat = zooKeeper.exists(path, watcher);
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private String createNode(String path,String data,CreateMode createMode) {
        String rePath = null;
        try {
            byte[] value = data == null ? null : data.getBytes(Charset.forName("UTF-8"));
            rePath = zooKeeper.create(path, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return rePath;
    }

    private List<String> getChildrenAndSort(String path) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(path, false);
            children.sort(String::compareTo);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return children;
    }


    private interface ElectionCallback {

        void becomeLeader(String leaderPath);
    }

    private class SeqNodeWatch implements Watcher {

        private String path;
        private ElectionCallback callback;

        public SeqNodeWatch(String path, ElectionCallback callback) {
            this.path = path;
            this.callback = callback;
        }

        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case NodeDeleted:
                    System.err.format("Node %s deleted or crash \n", event.getPath());
                    List<String> children = getChildrenAndSort(rootPath);
                    boolean flag = clientNodeCompareChildren(path, children);
                    if (flag) {
                        String watchPath = getPrevPath(path,children);
                        exists(watchPath, new SeqNodeWatch(path, callback));
                    } else {
                        if (exists(path,null)) {
                            System.err.format("Node %s become leader in watch\n", path);
                            if (callback == null)
                                leaderLatch.countDown();
                            else
                                callback.becomeLeader(path);
                        } else {
                            System.err.format("Node %s already deleted\n",path);
                        }
                    }
                    break;
                default:
                    break;
            }

        }
    }

    private boolean clientNodeCompareChildren(String path, List<String> children) {
        String seqNum = getPathSeqNumber(path);
        Integer seq = Integer.parseInt(seqNum);
        Integer minChildren = Integer.parseInt(children.get(0));
        return seq > minChildren;
    }

    private String getPathSeqNumber(String path) {
        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }

    private String getPrevPath(String path, List<String> children) {
        int index = children.indexOf(getPathSeqNumber(path));
        return rootPath + "/" + children.get(index - 1);
    }

    //path为自己创建的节点
    private boolean leaderElection(String path, ElectionCallback callback) throws KeeperException, InterruptedException {
        List<String> children = getChildrenAndSort(rootPath);
        boolean flag = clientNodeCompareChildren(path ,children);
        if (flag) { //自己创建的节点比排序后的children中第一个(最小)大,watch前一个node
            String watchPath = getPrevPath(path,children);
            System.err.format("%s 比children中最小的大 开始watch前一个node %s,等待...\n", path, watchPath);
            exists(watchPath, new SeqNodeWatch(path, callback));
            if (callback == null) {
                leaderLatch.await();//阻塞等待watch得到通知并再次选举leader成功
                return true;
            } else {
                return false;
            }
        } else { //自己创建的节点比第一个(最小)小，或者等于,既成为leader
            System.err.format("%s become leader\n", path);
            return true;
        }
    }

    public boolean startByBlocking() {
        String path = createNode(rootPath + "/", null, CreateMode.EPHEMERAL_SEQUENTIAL);
        try {
            return leaderElection(path,null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean startByCallback(ElectionCallback callback) {
        String path = createNode(rootPath + "/", null, CreateMode.EPHEMERAL_SEQUENTIAL);
        try {
            return leaderElection(path,callback);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }



    public static void main(String[] args) throws InterruptedException {
        CountDownLatch mainLatch = new CountDownLatch(1);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(3);
        ZkLeaderElectionFair leaderElectionFair = new ZkLeaderElectionFair("127.0.0.1:2181",8000,"/leaderRoot");

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        for (int i =0;i< 3;i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println(Thread.currentThread().getName() + ":waiting");
                        cyclicBarrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + ":do work");
                    boolean b = leaderElectionFair.startByBlocking();
                    System.out.println(Thread.currentThread().getName() + ":" + b);
                }
            });
        }
        executorService.shutdown();
        mainLatch.await();
    }
}

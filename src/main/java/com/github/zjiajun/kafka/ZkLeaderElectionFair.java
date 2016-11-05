package com.github.zjiajun.kafka;

import com.github.zjiajun.kafka.zk.NodeInfo;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private NodeInfo nodeInfo;
    private ZooKeeper zooKeeper;
    private final String rootPath;
    private final ElectionCallback callback;
    private final CountDownLatch initZkLatch = new CountDownLatch(1);
    private final CountDownLatch leaderLatch = new CountDownLatch(1);

    private interface ElectionCallback {

        void becomeLeader(String leaderPath);
    }

    public ZkLeaderElectionFair(String connectString, int sessionTimeout, String rootPath, ElectionCallback callback) {
        this.rootPath = rootPath;
        this.callback = callback;
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

    private List<String> getChildren(String path) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return children;
    }


    private class PrevNodeWatch implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (Event.EventType.NodeDeleted.equals(event.getType())) {
                System.err.format("%s delete\n", event.getPath());
                String nodePath = nodeInfo.getNodePath();
                if (!event.getPath().equals(nodePath)) {
                    if (!exists(nodePath, null)) {
                        System.err.format("Node %s already delete\n",nodePath);
                        leaderLatch.countDown();
                        return;
                    }
                    List<NodeInfo> childNodeInfo = makeChildNodeInfo();
                    if (nodeInfo.getId().equals(childNodeInfo.get(0).getId())) {
                        System.err.format("Node %s become leader in watch\n", nodePath);
                        if (callback == null)
                            leaderLatch.countDown();
                        else
                            callback.becomeLeader(nodePath);

                        System.err.format("%s become leader\n", nodePath);
                    } else {
                        String prevWatchPath = getPrevPath(childNodeInfo);
                        System.err.format("%s 比childNode中最小的%s 大,开始watch前一个node %s,等待...\n", nodePath, childNodeInfo.get(0).getNodePath(),prevWatchPath);
                        exists(prevWatchPath, new PrevNodeWatch());
                    }
                }
                System.err.println("nodePath: " + nodePath);
            }
        }
    }


    private String getPathSeqNumber(String path) {
        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }

    private String getPrevPath(List<NodeInfo> childNodeInfo) {
        //利用NodeInfo的equals判断
        int index = childNodeInfo.indexOf(nodeInfo);
        if (index > 0)
            return childNodeInfo.get(index - 1).getNodePath(); //获取前一个path
        throw new RuntimeException("getPrevPath error");
    }

    private boolean leaderElection(boolean isWatchHandler) throws KeeperException, InterruptedException {
        boolean isSuccess = false;
        String nodePath = nodeInfo.getNodePath();
        System.err.println("nodePath: " + nodePath);
        List<NodeInfo> childNodeInfo = makeChildNodeInfo();
        if (nodeInfo.getId().equals(childNodeInfo.get(0).getId())) {
            System.err.format("%s become leader\n", nodePath);
            isSuccess = true;
        } else {
            String prevWatchPath = getPrevPath(childNodeInfo);
            System.err.format("%s 比childNode中最小的%s 大,开始watch前一个node %s,等待...\n", nodePath, childNodeInfo.get(0).getNodePath(),prevWatchPath);
            exists(prevWatchPath, new PrevNodeWatch());
            if (callback == null) {
                System.err.format("%s begin await\n",nodePath);
                leaderLatch.await();//阻塞等待watch得到通知并再次选举leader成功
                //得到通知后，还需要再判断下nodePath是否存在，因为可能宕机的节点也会收到删除节点通知
                isSuccess = exists(nodePath, null);
            }
        }
        return isSuccess;
    }

    /**
     * 本机生成的node信息
     */
    private void makeNodeInfo() {
        String nodePath = createNode(rootPath + "/", null, CreateMode.EPHEMERAL_SEQUENTIAL);
        Integer id = Integer.parseInt(getPathSeqNumber(nodePath));
        nodeInfo = new NodeInfo(id,nodePath);
    }

    /**
     * 根节点rootPath下的子节点
     *
     * @return childNode 排序集合
     */
    private List<NodeInfo> makeChildNodeInfo() {
        List<String> childNode = getChildren(rootPath);
        List<NodeInfo> childNodeInfo = new ArrayList<>(childNode.size());

        for (String childNodePath : childNode) {
            Integer id = Integer.parseInt(getPathSeqNumber(childNodePath));
            String nodePath = rootPath + "/" + childNodePath;
            childNodeInfo.add(new NodeInfo(id, nodePath));
        }
        //从小到大排序
        childNodeInfo.sort((o1, o2) -> o1.getId().compareTo(o2.getId()));
        return childNodeInfo;
    }


    public boolean start(boolean isBlocking) {
        if (isBlocking) Objects.requireNonNull(callback, "ElectionCallback must be not empty");
        try {
            makeNodeInfo();
            return leaderElection(false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch mainLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        ElectionCallback callback = new ElectionCallback() {

            @Override
            public void becomeLeader(String leaderPath) {
                System.err.format("callback %s leader\n",leaderPath);
            }
        };

        for (int i =0;i< 3;i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    ZkLeaderElectionFair leaderElectionFair = new ZkLeaderElectionFair("127.0.0.1:2181", 8000, "/leaderRoot", null);
                    boolean b = leaderElectionFair.start(false);
                    System.out.println(Thread.currentThread().getName() + ":" + b);
                }
            });
        }


        executorService.shutdown();
        mainLatch.await();
    }
}

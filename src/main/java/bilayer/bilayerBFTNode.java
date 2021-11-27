package bilayer;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.TimerManager;

import static constant.ConstantValue.*;

public class bilayerBFTNode {

    Logger logger = LoggerFactory.getLogger(getClass());

    private int n;                                                  // 总节点数
    private int maxF;                                               // 最大容错数, 对应论文中的f
    private int index;                                              // 该节点的标识
    private bilayerBFTMsg curREQMsg;                                // 当前正在处理的请求
    private long SendWeightTime = SEND_WEIGHT_TIME;                 // 隔多久发送WEIGHT消息, 根据实际情况调整
    private long SendNoBlockTime = SEND_NO_BLOCK_TIME;              // 隔多久发送NO_BLOCK消息, 根据实际情况调整
    public double totalSendMsgLen = 0;                              // 发送的所有消息的长度之和
    private volatile boolean isRunning = false;                     // 是否正在运行, 可用于设置Crash节点

    private int groupSize;                                          // 组大小
    private boolean isLeader;                                       // 是否是leader
    private int weight;                                             // 用于是leader时的权重记录


    // 消息队列
    private BlockingQueue<bilayerBFTMsg> qbm = Queues.newLinkedBlockingQueue();

    // RBC解码后区块记录
    private Set<String> RBCMsgRecord = Sets.newConcurrentHashSet();

    // 准备阶段消息记录
    private Set<String> PAMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的PA消息对应的数量
    private AtomicLongMap<String> PAMsgCountMap = AtomicLongMap.create();

    // 提交阶段消息记录
    private Set<String> CMMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的CM消息对应的数量
    private AtomicLongMap<String> CMMsgCountMap = AtomicLongMap.create();

    // 回复消息数量
    private AtomicLong replyMsgCount = new AtomicLong();

    // 已经成功处理过的请求
    private Map<String,bilayerBFTMsg> doneMsgRecord = Maps.newConcurrentMap();

    // 存入client利用RBC发出区块的时间, 用于判断何时发送WEIGHT和NO_BLOCK消息
    private Map<String,Long> RBCStartTime = Maps.newHashMap();

    // 请求队列
    private BlockingQueue<bilayerBFTMsg> reqQueue = Queues.newLinkedBlockingDeque();

    // 权重累加值
    private AtomicLong WeightSum = new AtomicLong();

    private Timer timer;

    public bilayerBFTNode(int index, int n, int groupSize, boolean isLeader) {
        this.index = index;
        this.n = n;
        this.maxF = (n-1) / 3;
        this.groupSize = groupSize;
        this.isLeader = isLeader;
        timer = new Timer("Timer"+index);
    }





    // 向所有节点广播 (组内组外)
    private synchronized void publishToAll(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]向所有节点广播消息:" + msg);
        for(int i = 0; i < n; i++) {
            send(bilayerBFTMain.node2Index[i], new bilayerBFTMsg(msg));
        }
    }

    // 组内广播
    private synchronized void publishInsideGroup(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]组内广播消息:" + msg);
        int leaderIndex = getLeaderIndex(index);
        for(int i = 0; i < groupSize; i++) {
            send(leaderIndex + i, new bilayerBFTMsg(msg));
        }
    }

    private synchronized void send(int toIndex, bilayerBFTMsg msg) {
        // 模拟发送时长
        try {
            Thread.sleep(sendMsgTime(msg, BANDWIDTH));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        totalSendMsgLen += msg.getMsgLen();

        // 模拟网络延迟
        TimerManager.schedule(() -> {
            bilayerBFTMain.nodes[bilayerBFTMain.index2Node[toIndex]].pushMsg(msg);
            return null;
        },bilayerBFTMain.netDelay[index][toIndex]);

    }

    public long sendMsgTime(bilayerBFTMsg msg, int bandwidth) {
        return msg.getMsgLen() * 1000 / bandwidth;
    }

    public void pushMsg(bilayerBFTMsg msg) {
        try {
            this.qbm.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean checkMsg(bilayerBFTMsg msg) {
        return (msg.isValid()
                // 如果是自己的消息无需校验
                // 收到的消息是自己组的
                && (msg.getSenderId() == index || getLeaderIndex(index) == getLeaderIndex(msg.getSenderId())));
    }

    // 为了方便, 将节点序号隔OFFSET个分为一组, 第一个能被OFFSET整除的序号对应的是leader
    public int getLeaderIndex(int index) {
        return index / OFFSET * OFFSET;
    }

    private void NodeCrash(){
        logger.info("[节点" + index + "]宕机--------------");
        this.isRunning = false;
    }

    private void NodeRecover() {
        logger.info("[节点" + index + "]恢复--------------");
        this.isRunning = true;
    }

}

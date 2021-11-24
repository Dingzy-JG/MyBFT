package bilayer;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.PBFTMsg;

public class bilayerBFTNode {

    Logger logger = LoggerFactory.getLogger(getClass());

    private int n;                                                  // 总节点数
    private int maxF;                                               // 最大容错数, 对应论文中的f
    private int index;                                              // 该节点的标识
    private bilayerBFTMsg curREQMsg;                                // 当前正在处理的请求
    private int groupSize;                                          // 组大小
    private long SendWeightTime;                                    // 隔多久发送WEIGHT消息
    private long SendNoBlockTime;                                   // 隔多久发送NO_BLOCK消息
    public double totalSendMsgLen = 0;                              // 发送的所有消息的长度之和
    private volatile boolean isRunning = false;                     // 是否正在运行, 可用于设置Crash节点

    private boolean isLeader;                                       // 是否是leader
    private long[] memberIds;                                       // 组员id, [0]对应的是leader
    private int weight;                                             // 用于是leader时的权重记录

    // 消息队列
    private BlockingQueue<PBFTMsg> qbm = Queues.newLinkedBlockingQueue();

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
    private Map<String,PBFTMsg> doneMsgRecord = Maps.newConcurrentMap();

    // 还未修改
//    // 存入client利用RBC发出区块的时间, 用于判断WEIGHT超时
//    private Map<String,Long> WeightTimer = Maps.newHashMap();
//    // 存入发请求消息的时间
//    // 如果请求超时，view加1，重试
//    private Map<String,Long> REQMsgTimeout = Maps.newHashMap();

    // 请求队列
    private BlockingQueue<PBFTMsg> reqQueue = Queues.newLinkedBlockingDeque();

    private Timer timer;


}

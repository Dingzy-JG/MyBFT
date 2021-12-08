package bilayer;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import enums.MessageEnum;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SignatureUtils;
import util.TimerManager;
import util.Utils;

import static constant.ConstantValue.*;

public class bilayerBFTNode {

    Logger logger = LoggerFactory.getLogger(bilayerBFTNode.class);

    private int n;                                                  // 总节点数
    private int maxF;                                               // 最大容错数, 对应论文中的f
    private int index;                                              // 该节点的标识
    private bilayerBFTMsg curREQMsg;                                // 当前正在处理的请求
    public double totalSendMsgLen = 0;                              // 发送的所有消息的长度之和
    private volatile boolean isRunning = false;                     // 是否正在运行, 可用于设置Crash节点

    private int groupSize;                                          // 组大小
    private int groupMaxF;                                          // 组中的容错数量
    private boolean isLeader;                                       // 是否是leader

    // 消息队列
    private BlockingQueue<bilayerBFTMsg> qbm = Queues.newLinkedBlockingQueue();

    // RBC解码后区块记录 (DataKey)
    private Set<String> REQMsgRecord = Sets.newConcurrentHashSet();

    // 准备阶段消息记录 (MsgKey)
    private Set<String> PAMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的PA消息对应的数量 <DataKey, count>
    private AtomicLongMap<String> PAMsgCountMap = AtomicLongMap.create();

    // 提交阶段消息记录 (MsgKey)
    private Set<String> CMMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的CM消息对应的数量 <DataKey, count>
    private AtomicLongMap<String> CMMsgCountMap = AtomicLongMap.create();

    // 记录收到的回复消息 (MsgKey)
    private Set<String> REPLYMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的REPLY消息对应的数量 <DataKey, count>
    private AtomicLongMap<String> REPLYMsgCountMap = AtomicLongMap.create();

    // 记录收到的NO_BLOCK消息 (MsgKey)
    private Set<String> NO_BLOCKMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的NO_BLOCK消息的数量 <DataKey, count>
    private AtomicLongMap<String> NO_BLOCKMsgCountMap = AtomicLongMap.create();

    // 记录收到的WABA消息, 因为MsgKey分辨不出轮数和b, 所以加上_r_b (MsgKey_r_b)
    private Set<String> WABAMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经发送过的WABA消息 (DataKey_r_b)
    private Set<String> haveSentWABA_B_Msg = Sets.newConcurrentHashSet();
    // 权重累加值 <DataKey_r_b, weightSum>
    private AtomicLongMap<String> WABAWeightSumMap = AtomicLongMap.create();

    // 记录收到的AUX消息, 因为MsgKey分辨不出轮数和u, 所以加上_r_u (MsgKey_r_u)
    private Set<String> AUXMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经发送过的AUX消息 (DataKey_r_u)
    private Set<String> haveSentAUX_U_Msg = Sets.newConcurrentHashSet();
    // AUX的权重累加值 <DataKey_r_u, weightSum>
    private AtomicLongMap<String> AUXWeightSumMap = AtomicLongMap.create();

    // 存储WABA是否已经decide <DataKey, decided>
    private Map<String, Boolean> decidedMap = Maps.newConcurrentMap();
    // 存储WABA的value_r集合 <DataKey_r, value_r>
    private Map<String, Set<Integer>> valueMap = Maps.newConcurrentMap();
    // 存储WABA的val_r集合 <DataKey_r, val_r>
    private Map<String, Set<Integer>> valMap = Maps.newConcurrentMap();

    // 记录收到的AFFIRM_HONEST消息 (MsgKey)
    private Set<String> AFFIRM_HONESTMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的NO_BLOCK消息的数量 <DataKey, count>
    private AtomicLongMap<String> AFFIRM_HONESTMsgCountMap = AtomicLongMap.create();

    // 存入client利用RBC发出区块的时间, 用于判断何时发送WEIGHT和NO_BLOCK消息 <DataKey, 开始时间戳>
    // 因为请求是一个一个执行的, 所以这个Map不需要使用并发的
    private Map<String, Long> REQTimer = Maps.newHashMap();

    // 已经成功处理过的请求 <DataKey, 完成时间戳>
    private Map<String, Long> doneTimer = Maps.newConcurrentMap();

    // 请求队列
    private BlockingQueue<bilayerBFTMsg> reqQueue = Queues.newLinkedBlockingDeque();

    private Timer timer;

    public bilayerBFTNode(int index, int n, int groupSize, boolean isLeader) {
        this.index = index;
        this.n = n;
        this.maxF = (n-1) / 3;
        this.groupSize = groupSize;
        this.groupMaxF = (groupSize-1) / 3;
        this.isLeader = isLeader;
        timer = new Timer("Timer"+index);
    }

    public bilayerBFTNode start() {
        new Thread(() -> {
            while(true) {
                try {
                    bilayerBFTMsg msg = qbm.take();
                    doAction(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        isRunning = true;
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                doReq();
//                checkTimer();
            }
        }, 100, 100);
        return this;
    }

    private boolean doAction(bilayerBFTMsg msg) {
        if(!isRunning) return false;
        if(msg != null) {
            logger.info("[节点" + index + "]收到消息:"+ msg);
            switch (msg.getType()) {
                case REQUEST:
                    onRequest(msg);
                    break;
                case PREPARE:
                    onPrepare(msg);
                    break;
                case COMMIT:
                    onCommit(msg);
                    break;
                case REPLY:
                    onReply(msg);
                    break;
                case WEIGHT:
                    onWeight(msg);
                    break;
                case NO_REPLY:
                    onNoReply(msg);
                    break;
                case NO_BLOCK:
                    onNoBlock(msg);
                    break;
                case WABA:
                    onWABA(msg);
                    break;
                case AUX:
                    onAUX(msg);
                    break;
                case WABA_RESULT:
                    onWABAResult(msg);
                    break;
                case PROOF_HONEST:
                    onProofHonest(msg);
                    break;
                case AFFIRM_HONEST:
                    onAffirmHonest(msg);
                    break;
                default:
                    break;
            }
            return true;
        }
        return false;
    }

    private void onRequest(bilayerBFTMsg msg) {
        if(!msg.isValid()) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        SignatureUtils.verify();
        if(REQMsgRecord.contains(msg.getDataKey())) return;
        REQMsgRecord.add(msg.getDataKey());
        // 根据第一层共识算法, 直接广播prepare消息
        bilayerBFTMsg PAMsg = new bilayerBFTMsg(msg);
        PAMsg.setType(MessageEnum.PREPARE);
        PAMsg.setSenderId(index);
        // 组内广播PA消息
        SignatureUtils.sign();
        publishInsideGroup(PAMsg);
    }

    private void onPrepare(bilayerBFTMsg msg) {
        if(!checkMsg(msg)) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        SignatureUtils.verify();
        String msgKey = msg.getMsgKey();
        // 说明已经投过票, 不能重复投
        if(PAMsgRecord.contains(msgKey)) return;
        // 记录收到的PAMsg
        PAMsgRecord.add(msgKey);
        // 票数+1, 并返回+1后的票数
        long agCou = PAMsgCountMap.incrementAndGet(msg.getDataKey());
        if(agCou >= 2*groupMaxF + 1) {
            PAMsgCountMap.remove(msg.getDataKey());
            bilayerBFTMsg CMMsg = new bilayerBFTMsg(msg);
            CMMsg.setType(MessageEnum.COMMIT);
            CMMsg.setSenderId(index);
            SignatureUtils.sign();
            publishInsideGroup(CMMsg);
        }
    }

    private void onCommit(bilayerBFTMsg msg) {
        if(!checkMsg(msg)) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        SignatureUtils.verify();
        String msgKey = msg.getMsgKey();
        // 已经投过票, 不能重复投
        if(CMMsgRecord.contains(msgKey)) return;
        // 必须先过准备阶段
        if(!PAMsgRecord.contains(msgKey)) return;
        // 记录收到的CMMsg
        CMMsgRecord.add(msgKey);
        // 票数+1, 并返回+1后的票数
        long agCou = CMMsgCountMap.incrementAndGet(msg.getDataKey());
        if(agCou == 2*groupMaxF + 1) { // 改成等于, 只在到2f+1的那一次发REPLY
            // 和PBFT不同, 这里先不执行请求, 只发送REPLY消息, 等到最终共识结果出来再执行请求
            bilayerBFTMsg REPLYMsg = new bilayerBFTMsg(msg);
            REPLYMsg.setType(MessageEnum.REPLY);
            REPLYMsg.setSenderId(index);
            // 发送REPLY消息给leader
            SignatureUtils.sign();
            send(getLeaderIndex(index), REPLYMsg);
        }
    }

    private void onReply(bilayerBFTMsg msg) {
        SignatureUtils.verify();
        String msgKey = msg.getMsgKey();
        String dataKey = msg.getDataKey();
        if(!REQMsgRecord.contains(dataKey)) return;
        if(REPLYMsgRecord.contains(msgKey)) return;
        REPLYMsgCountMap.incrementAndGet(dataKey);
        REPLYMsgRecord.add(msgKey);
    }

    private void onWeight(bilayerBFTMsg msg) {
        SignatureUtils.verify();
        String dataKey = msg.getDataKey();
        long weight_1 = REPLYMsgCountMap.get(dataKey);
        if(weight_1 != 0) {
            // 组内接受到了能通过验证的reply消息, 在主节点之间广播对应的WABA消息
            bilayerBFTMsg WABA_1_Msg = new bilayerBFTMsg(msg);
            WABA_1_Msg.setType(MessageEnum.WABA);
            WABA_1_Msg.setSenderId(index);
            WABA_1_Msg.setR(0);
            WABA_1_Msg.setB(1);
            WABA_1_Msg.setWeight(weight_1);
            // 因为可能存在加起来权重超过了f+1直接发过了, 不判断的话可能存在重复发送
            // "_0_1": 前一个代表r, 后一个代表b
            if(!haveSentWABA_B_Msg.contains(dataKey + "_0_1")) {
                SignatureUtils.sign();
                publishToLeaders(WABA_1_Msg);
                haveSentWABA_B_Msg.add(dataKey + "_0_1");
            }
        } else {
            // 否则就向组员广播NO_REPLY消息, 收集组员投票为0的签名碎片
            bilayerBFTMsg NO_REPLYMsg = new bilayerBFTMsg(msg);
            NO_REPLYMsg.setType(MessageEnum.NO_REPLY);
            NO_REPLYMsg.setSenderId(index);
            SignatureUtils.sign();
            publishInsideGroup(NO_REPLYMsg);

            // 达到指定的收集NO_BLOCK时间后, 发送内容为0的WABA消息
            TimerManager.schedule(() -> {
                bilayerBFTMsg WABA_0_Msg = new bilayerBFTMsg(msg);
                long weight_0 = NO_BLOCKMsgCountMap.get(dataKey);
                WABA_0_Msg.setType(MessageEnum.WABA);
                WABA_0_Msg.setSenderId(index);
                WABA_0_Msg.setR(0);
                WABA_0_Msg.setB(0);
                WABA_0_Msg.setWeight(weight_0);
                if(!haveSentWABA_B_Msg.contains(dataKey + "_0_0")) {
                    SignatureUtils.sign();
                    publishToLeaders(WABA_0_Msg);
                    haveSentWABA_B_Msg.add(dataKey + "_0_0");
                }
                return null;
            }, GATHER_NO_BLOCK_TIME);
        }
    }

    private void onNoReply(bilayerBFTMsg msg) {
        if(REQMsgRecord.contains(msg.getDataKey())) return;
        SignatureUtils.verify();
        // 没有收到对应的区块, 则向其leader发送NO_BLOCK消息
        bilayerBFTMsg NO_BLOCKMsg = new bilayerBFTMsg(msg);
        NO_BLOCKMsg.setType(MessageEnum.NO_BLOCK);
        NO_BLOCKMsg.setSenderId(index);
        NO_BLOCKMsg.setB(0);
        SignatureUtils.sign();
        send(getLeaderIndex(index), NO_BLOCKMsg);
    }

    private void onNoBlock(bilayerBFTMsg msg) {
        String msgKey = msg.getMsgKey();
        if(NO_BLOCKMsgRecord.contains(msgKey)) return;
        SignatureUtils.verify();
        NO_BLOCKMsgCountMap.incrementAndGet(msg.getDataKey());
        NO_BLOCKMsgRecord.add(msgKey);
    }

    private void onWABA(bilayerBFTMsg msg) {
        String msgKey_r_b = msg.getMsgKey() + msg.getR() + msg.getB();
        String dataKey = msg.getDataKey();
        String dataKey_r = dataKey + "_" + msg.getR();
        String dataKey_r_b = dataKey_r + "_" + msg.getB();
        if(WABAMsgRecord.contains(msgKey_r_b)) return;
        SignatureUtils.verify();
        long weight = msg.getWeight();
        long WABAWeightSum = WABAWeightSumMap.addAndGet(dataKey_r_b, weight);
        WABAMsgRecord.add(msgKey_r_b);
        // 权重之和大于等于f+1, 且未发送对应的WABA
        if(WABAWeightSum >= maxF + 1 && !haveSentWABA_B_Msg.contains(dataKey_r_b)) {
            bilayerBFTMsg WABA_B_Msg = new bilayerBFTMsg(msg);
            // 类型已经是WABA, r和b也和收到的msg中的一致
            WABA_B_Msg.setSenderId(index);
            WABA_B_Msg.setWeight(REPLYMsgCountMap.get(dataKey));
            SignatureUtils.sign();
            publishToLeaders(WABA_B_Msg);
            haveSentWABA_B_Msg.add(dataKey_r_b);
        }
        // 权重之和大于等于2f+1
        if(WABAWeightSum >= 2*maxF + 1) {
            if(!valueMap.containsKey(dataKey_r)) {
                Set<Integer> valueSet = new HashSet();
                valueSet.add(msg.getB());
                valueMap.put(dataKey_r, valueSet);
            } else {
                valueMap.get(dataKey_r).add(msg.getB());
            }
        }
        // 当value_r集合不为空时
        Set<Integer> value_r = valueMap.get(dataKey_r);
        if(value_r != null && value_r.size() != 0) {
            for(Integer u: value_r) {
                String dataKey_r_u = dataKey_r + u;
                // 防止重复发送
                if(!haveSentAUX_U_Msg.contains(dataKey_r_u)) {
                    bilayerBFTMsg AUXMsg = new bilayerBFTMsg(msg);
                    // r与收到的msg一致
                    AUXMsg.setType(MessageEnum.AUX);
                    AUXMsg.setSenderId(index);
                    AUXMsg.setB(u);
                    AUXMsg.setWeight(REPLYMsgCountMap.get(dataKey));
                    SignatureUtils.sign();
                    publishToLeaders(AUXMsg);
                    haveSentAUX_U_Msg.add(dataKey_r_u);
                }
            }
        }
    }

    private void onAUX(bilayerBFTMsg msg) {
        String msgKey_r_u = msg.getMsgKey() + msg.getR() + msg.getB();
        String dataKey = msg.getDataKey();
        String dataKey_r = dataKey + "_" + msg.getR();
        String dataKey_r_u = dataKey_r + "_" + msg.getB();
        if(AUXMsgRecord.contains(msgKey_r_u)) return;
        SignatureUtils.verify();
        long weight = msg.getWeight();
        long AUXWeightSum = AUXWeightSumMap.addAndGet(dataKey_r_u, weight);
        AUXMsgRecord.add(msgKey_r_u);
        // 权重之和大于等于n-f
        if(AUXWeightSum >= n - maxF) {
            if(!valMap.containsKey(dataKey_r)) {
                Set<Integer> valSet = new HashSet();
                valSet.add(msg.getB());
                valMap.put(dataKey_r, valSet);
            } else {
                valMap.get(dataKey_r).add(msg.getB());
            }
        }
        // 当集合中只有一个元素b时
        Set<Integer> val_r = valMap.get(dataKey_r);
        if(val_r != null) {
            int valSize = val_r.size();
            // s = H(H(m)|r)
            // 因为dataKey中包含了时间戳, 所以每次运行结果不一样
            byte[] s = Utils.getSHA256(dataKey + msg.getR());
            if(valSize == 1) {
                for(Integer b: val_r) {
                    // 有时会小于0
                    int tempS = (s[s.length-1] % 2 + 2) % 2;
                    if(b == tempS) {
                        Boolean decided = decidedMap.get(dataKey);
                        if(decided != null && decided == true) {
                            return;
                        }
                        else {
                            bilayerBFTMsg RESULTMsg = new bilayerBFTMsg(msg);
                            RESULTMsg.setType(MessageEnum.WABA_RESULT);
                            RESULTMsg.setSenderId(index);
                            RESULTMsg.setB(b);
                            SignatureUtils.sign();
                            publishInsideGroup(RESULTMsg);
                            decidedMap.put(dataKey, true);
                        }
                    } else {
                        int r = msg.getR() + 1;
                        if(!haveSentWABA_B_Msg.contains(dataKey + "_" + r + "_" + b)) {
                            bilayerBFTMsg WABAMsg = new bilayerBFTMsg(msg);
                            WABAMsg.setType(MessageEnum.WABA);
                            WABAMsg.setSenderId(index);
                            WABAMsg.setR(r);
                            WABAMsg.setB(b);
                            WABAMsg.setWeight(REPLYMsgCountMap.get(dataKey));
                            SignatureUtils.sign();
                            publishToLeaders(WABAMsg);
                            haveSentWABA_B_Msg.add(dataKey + "_" + r + "_" + b);
                        }
                    }
                }
            } else if(valSize == 2) {
                int b = s[s.length-1] % 2;
                int r = msg.getR() + 1;
                if(!haveSentWABA_B_Msg.contains(dataKey + "_" + r + "_" + b)) {
                    bilayerBFTMsg WABAMsg = new bilayerBFTMsg(msg);
                    WABAMsg.setType(MessageEnum.WABA);
                    WABAMsg.setSenderId(index);
                    WABAMsg.setR(r);
                    WABAMsg.setB(b);
                    WABAMsg.setWeight(REPLYMsgCountMap.get(dataKey));
                    SignatureUtils.sign();
                    publishToLeaders(WABAMsg);
                    haveSentWABA_B_Msg.add(dataKey + "_" + r + "_" + b);
                }
            }
        }
    }

    private void onWABAResult(bilayerBFTMsg msg) {
        SignatureUtils.verify();
        int b = msg.getB();
        if(b == 0) {
            discardBlock(msg);
        } else if(b == 1) {
            executeBlock(msg);
            if(msg.getPrimeNodeId() == index) {
                doneTimer.put(msg.getDataKey(), System.currentTimeMillis());
                logger.info("[节点" + index + "]: 摘要为{" + msg.getDataHash() + "}的区块消息确认成功!");

                // =====================================用于计时=====================================
                bilayerBFTMain.countDownLatch.countDown();
                // =====================================用于计时=====================================

                // 当前请求执行完成
                curREQMsg = null;
            }
        }
    }

    private void onProofHonest(bilayerBFTMsg msg) {
        if(!REQMsgRecord.contains(msg.getDataKey())) return;
        SignatureUtils.verify();
        bilayerBFTMsg AFFIRM_HONESTMsg = new bilayerBFTMsg(msg);
        AFFIRM_HONESTMsg.setType(MessageEnum.AFFIRM_HONEST);
        AFFIRM_HONESTMsg.setSenderId(index);
        SignatureUtils.sign();
        send(msg.getPrimeNodeId(), AFFIRM_HONESTMsg);
    }

    private void onAffirmHonest(bilayerBFTMsg msg) {
        String msgKey = msg.getMsgKey();
        if(AFFIRM_HONESTMsgRecord.contains(msgKey)) return;
        SignatureUtils.verify();
        AFFIRM_HONESTMsgCountMap.incrementAndGet(msg.getDataKey());
        AFFIRM_HONESTMsgRecord.add(msgKey);
        // 收集到f+1个就代表该节点是诚实的
    }

    // 执行对应请求
    private void executeBlock(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]成功执行摘要为{" + msg.getDataHash() + "}的区块请求");
    }

    private void discardBlock(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]丢弃摘要为{" + msg.getDataHash() + "}的区块");
    }

    // 请求入列
    public void req(String data) {
        bilayerBFTMsg REQMsg = new bilayerBFTMsg(MessageEnum.REQUEST, index);
        REQMsg.setDataHash(data);
        SignatureUtils.sign();
        try {
            reqQueue.put(REQMsg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 检查请求
    private boolean doReq() {
        if(curREQMsg != null) return false; // 上一个请求还未完成
        curREQMsg = reqQueue.poll();
        if(curREQMsg == null) return false;
        doSendCurMsg();
        return true;
    }

    // 发送当前请求
    private void doSendCurMsg() {
        // 记录通过RBC发送时间, 用于后续判断何时发送WEIGHT和NO_BLOCK消息
        String dataKey = curREQMsg.getDataKey();
        REQTimer.put(dataKey, System.currentTimeMillis());

        // 达到指定时间后发送WEIGHT消息
        TimerManager.schedule(() -> {
            bilayerBFTMsg WEIGHTMsg = new bilayerBFTMsg(curREQMsg);
            WEIGHTMsg.setType(MessageEnum.WEIGHT);
            SignatureUtils.sign();
            publishToLeaders(WEIGHTMsg);
            return null;
        }, SEND_WEIGHT_TIME);

        TimerManager.schedule(() -> {
            // 如果超过指定之间还没完成请求
            if(!doneTimer.containsKey(dataKey)) {
                bilayerBFTMsg PROOF_HONESTMsg = new bilayerBFTMsg(curREQMsg);
                PROOF_HONESTMsg.setType(MessageEnum.PROOF_HONEST);
                SignatureUtils.sign();
                publishToAll(PROOF_HONESTMsg);
            }
            return null;
        }, SEND_PROOF_HONEST_TIME);

        doRBC();
    }

    // 通过RBC把请求发给所有节点, 这一步其实相当于PBFT中的pre-prepare阶段
    private void doRBC() {
        // TODO 补充RBC, 效果为所有节点的qbm中加入请求
        publishToAll(curREQMsg);
    }

    private void checkTimer() {
    }

    // 向所有节点广播 (组内组外)
    public synchronized void publishToAll(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]向所有节点广播消息:" + msg);
        for(int i = 0; i < n; i++) {
            send(bilayerBFTMain.node2Index[i], new bilayerBFTMsg(msg));
        }
    }

    // 向所有leaders广播消息
    public synchronized void publishToLeaders(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]向所有leaders广播消息:" + msg);
        for(int i = 0; i < n; i++) {
            if(bilayerBFTMain.nodes[i].isLeader) {
                send(bilayerBFTMain.nodes[i].index, new bilayerBFTMsg(msg));
            }
        }
    }

    // 组内广播
    public synchronized void publishInsideGroup(bilayerBFTMsg msg) {
        logger.info("[节点" + index + "]组内广播消息:" + msg);
        int leaderIndex = getLeaderIndex(index);
        for(int i = 0; i < groupSize; i++) {
            send(leaderIndex + i, new bilayerBFTMsg(msg));
        }
    }

    public synchronized void send(int toIndex, bilayerBFTMsg msg) {
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
            qbm.put(msg);
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

    private void cleanCache(String it) {
        PAMsgRecord.removeIf((vp) -> StringUtils.startsWith(vp, it));
        CMMsgRecord.removeIf((vp) -> StringUtils.startsWith(vp, it));
        PAMsgCountMap.remove(it);
        CMMsgRecord.remove(it);
    }

    // 为了方便, 将节点序号隔OFFSET个分为一组, 第一个能被OFFSET整除的序号对应的是leader
    public int getLeaderIndex(int index) {
        if(n < 32) return 0;
        return index / OFFSET * OFFSET;
    }

    private void NodeCrash() {
        logger.info("[节点" + index + "]宕机--------------");
        isRunning = false;
    }

    private void NodeRecover() {
        logger.info("[节点" + index + "]恢复--------------");
        isRunning = true;
    }

}

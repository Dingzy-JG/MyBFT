package pbft;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import enums.MessageEnum;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.TimerManager;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static constant.ConstantValue.*;

public class PBFTNode {

    Logger logger = LoggerFactory.getLogger(getClass());

    private int n;                                                  // 总节点数
    private int maxF;                                               // 最大容错数, 对应论文中的f
    private int index;                                              // 该节点的标识
    private int view;                                               // 该节点当前所处的视图
    private PBFTMsg curREQMsg;                                      // 当前正在处理的请求
    private volatile boolean viewOK;                                // 所处视图的状态
    private volatile boolean isRunning = false;                     // 是否正在运行, 可用于设置Crash节点
    private long timeout = INIT_TIMEOUT;                            // 超时计时器
    public double totalSendMsgLen = 0;                              // 发送的所有消息的长度之和

    // 消息队列
    private BlockingQueue<PBFTMsg> qbm = Queues.newLinkedBlockingQueue();

    // 预准备阶段消息记录
    private Set<String> PPMsgRecord = Sets.newConcurrentHashSet();

    // 准备阶段消息记录
    private Set<String> PAMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的PA消息对应的数量
    private AtomicLongMap<String> PAMsgCountMap = AtomicLongMap.create();

    // 提交阶段消息记录
    private Set<String> CMMsgRecord = Sets.newConcurrentHashSet();
    // 记录已经收到的CM消息对应的数量
    private AtomicLongMap<String> CMMsgCountMap = AtomicLongMap.create();

    // 回复消息数量
    // 这边应为每个节点最多同时处理一个请求, 所以记录reply时只用一个原子类计数即可
    private AtomicLong replyMsgCount = new AtomicLong();

    // 视图转换消息记录
    private Set<String> VCMsgRecord = Sets.newConcurrentHashSet();
    // 记录各个视图对应的消息数量
    private AtomicLongMap<Integer> VCMsgCountMap = AtomicLongMap.create();

    // 作为主节点时已经受理过的请求
    private Map<String, PBFTMsg> applyMsgRecord = Maps.newConcurrentMap();
    // 作为非主节点时成功处理过的请求
    private Map<String, PBFTMsg> doneMsgRecord = Maps.newConcurrentMap();

    // 存入发PPMsg的时间, 用于判断PBFT超时
    private Map<String, Long> PBFTMsgTimeout = Maps.newHashMap();
    // 存入发请求消息的时间
    // 如果请求超时, view加1, 重试
    private Map<String, Long> REQMsgTimeout = Maps.newHashMap();

    // 请求队列
    private BlockingQueue<PBFTMsg> reqQueue = Queues.newLinkedBlockingDeque();

    // 生成序列号
    private volatile AtomicInteger genSeqNo = new AtomicInteger(0);

    private Timer timer;

    public PBFTNode(int index, int n) {
        this.index = index;
        this.n = n;
        this.maxF = (n-1) / 3;
        timer = new Timer("Timer"+index);
    }

    public PBFTNode start() {
        new Thread(() -> {
            // 一直执行
            // 之后自己修改可以改成把请求队列中的内容执行完当做判断条件
            while(true) {
                try {
                    PBFTMsg msg = qbm.take();
                    doAction(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        isRunning = true;
        timer.schedule(new TimerTask() {
            int co = 0;
            @Override
            public void run() {
                if(co == 0) {
                    // 启动后先同步视图
                    pubView();
                }
                co++;

                doReq();
                checkTimer();
            }
        }, 100, 100);
        // 从请求队列中取出对应的来请求, 频率为100ms
        return this;
    }

    private boolean doAction(PBFTMsg msg) {
        if(!isRunning) return false;
        if(msg != null) {
            logger.info("[节点" + index + "]收到消息:"+ msg);
            switch (msg.getType()) {
                case REQUEST:
                    onRequest(msg);
                    break;
                case PRE_PREPARE:
                    onPrePrepare(msg);
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
                case VIEW:
                    onView(msg);
                    break;
                case VIEW_CHANGE:
                    onChangeView(msg);
                    break;
                default:
                    break;
            }
            return true;
        }
        return false;
    }

    private void onRequest(PBFTMsg msg) {
        if(!msg.isValid()) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        PBFTMsg newMsg = new PBFTMsg(msg);
        newMsg.setSenderId(index);
        // 视图号已过期
        if(msg.getViewNo() < view) return;

        // 记录第一次收到请求的时间
        // =====================================用于计时=====================================
        if(PBFTMain.startTime == 0) PBFTMain.startTime = System.currentTimeMillis();
        // =====================================用于计时=====================================

        if(msg.getViewNo() == index) { // 如果为主节点
            if(applyMsgRecord.containsKey(msg.getDataKey())) return; // 已经受理过
            applyMsgRecord.put(msg.getDataKey(), msg);
            // 主节点收到client的请求后进行广播
            newMsg.setType(MessageEnum.PRE_PREPARE);
            // 主节点生成序列号
            int seqNo = genSeqNo.incrementAndGet();
            newMsg.setSeqNo(seqNo);
            publish(newMsg);
        }else if(msg.getSenderId() != index) { // 忽略自己发的请求
            // 非主节点收到, 说明主节点可能宕机
            if(doneMsgRecord.containsKey(msg.getDataKey())) {
                // 已经处理过, 直接回复
                newMsg.setType(MessageEnum.REPLY);
                send(msg.getSenderId(), newMsg);
            }else{
                // 认为客户端进行了VC投票
                VCMsgRecord.add(msg.getSenderId()+"|@|"+(msg.getViewNo()+1));
                VCMsgCountMap.incrementAndGet(msg.getViewNo()+1);
                // 未处理, 说明可能主节点宕机, 转发给主节点试试
                logger.info("[节点" + index + "]转发给主节点:"+ msg);
                send(getPrimeIndex(view), newMsg);
                REQMsgTimeout.put(msg.getDataHash(), System.currentTimeMillis());
            }
        }
    }

    private void onPrePrepare(PBFTMsg msg) {
        if(!checkMsg(msg,true)) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        String msgKey = msg.getDataKey();
        if(PPMsgRecord.contains(msgKey)) {
            // 说明已经发起过, 不能重复发起
            return;
        }
        // 记录收到的PPMsg
        PPMsgRecord.add(msgKey);
        // 启动超时控制
        PBFTMsgTimeout.put(msgKey, System.currentTimeMillis());
        // 假如有请求的话, 移除请求超时, 用于处理上一次超时的同样请求
        REQMsgTimeout.remove(msg.getDataHash());
        // 进入准备阶段
        PBFTMsg PAMsg = new PBFTMsg(msg);
        PAMsg.setType(MessageEnum.PREPARE);
        PAMsg.setSenderId(index);
        publish(PAMsg);
    }

    private void onPrepare(PBFTMsg msg) {
        if(!checkMsg(msg,false)) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        String msgKey = msg.getMsgKey();
        if(PAMsgRecord.contains(msgKey)) {
            // 说明已经投过票, 不能重复投
            return;
        }
        if(!PPMsgRecord.contains(msg.getDataKey())) {
            // 必须之前发过预准备消息
            return;
        }
        // 记录收到的PAMsg
        PAMsgRecord.add(msgKey);
        // 票数+1, 并返回+1后的票数
        long agCou = PAMsgCountMap.incrementAndGet(msg.getDataKey());
        if(agCou >= 2*maxF + 1) {
            // 删除并返回与key关联的值, 如果不存在则返回0
            PAMsgCountMap.remove(msg.getDataKey());
            // 进入提交阶段
            PBFTMsg CMMsg = new PBFTMsg(msg);
            CMMsg.setType(MessageEnum.COMMIT);
            CMMsg.setSenderId(index);
            doneMsgRecord.put(CMMsg.getDataKey(), CMMsg);
            publish(CMMsg);
        }
        // 后续的票数肯定凑不满, 超时自动清除
    }

    private void onCommit(PBFTMsg msg) {
        if(!checkMsg(msg,false)) {
            logger.info("[节点" + index + "]收到异常消息" + msg);
            return;
        }
        String msgKey = msg.getMsgKey();
        if(CMMsgRecord.contains(msgKey)) {
            // 说明已经投过票, 不能重复投
            return;
        }
        if(!PAMsgRecord.contains(msgKey)) {
            // 必须先过准备阶段
            return;
        }
        // 记录收到的CMMsg
        CMMsgRecord.add(msgKey);
        // 票数+1, 并返回+1后的票数
        long agCou = CMMsgCountMap.incrementAndGet(msg.getDataKey());
        if(agCou >= 2*maxF + 1) {
            // 执行请求, 清空对应的PBFTMsg, 因为PBFT收到足够数量的commit就代表成功, 可以执行
            // 不清除的话, 超过2f+1之后, 每收到一次就发送一个REPLY了
            cleanCache(msg.getDataKey());
            if(msg.getSenderId() != index) {
                // 更新序列号
                genSeqNo.set(msg.getSeqNo());
            }
            // 进入回复阶段
            PBFTMsg REPLYMsg = new PBFTMsg(msg);
            REPLYMsg.setType(MessageEnum.REPLY);
            REPLYMsg.setSenderId(index);
            doSomething(REPLYMsg);
            // 发送REPLY消息给客户端
            send(REPLYMsg.getPrimeNodeId(), REPLYMsg);
        }
    }

    private void onReply(PBFTMsg msg) {
        if(curREQMsg == null || !curREQMsg.getDataHash().equals(msg.getDataHash())) return;
        long count = replyMsgCount.incrementAndGet();
        if(count >= maxF+1) {
            logger.info("消息确认成功[" + index + "]:" + msg);

            // =====================================用于计时=====================================
            PBFTMain.countDownLatch.countDown();
            // =====================================用于计时=====================================

            replyMsgCount.set(0);
            curREQMsg = null; // 当前请求已经完成
        }
    }

    // 视图初始化
    private void onView(PBFTMsg msg) {
        if(msg.getDataHash() == null) {
            // 最开始的初始化
            PBFTMsg sed = new PBFTMsg(msg);
            sed.setSenderId(index);
            sed.setViewNo(view);
            sed.setDataHash("initView");
            send(msg.getSenderId(), sed);
        }else{
            // 响应
            if(viewOK) return; // 已经初始化成功
            long count = VCMsgCountMap.incrementAndGet(msg.getViewNo());
            if(count >= 2* maxF +1) {
                VCMsgCountMap.clear();
                view = msg.getViewNo();
                viewOK = true;
                logger.info("[节点" + index + "]视图初始化完成:" + view);
            }
        }
    }

    private void onChangeView(PBFTMsg msg) {
        // 收集视图变更
        String VCMsgKey = msg.getSenderId()+"@"+msg.getViewNo();
        if(VCMsgRecord.contains(VCMsgKey)) {
            return;
        }
        VCMsgRecord.add(VCMsgKey);
        long count = VCMsgCountMap.incrementAndGet(msg.getViewNo());
        if(count >= 2*maxF + 1) {
            VCMsgCountMap.clear();
            view = msg.getViewNo();
            viewOK = true;
            logger.info("[节点" + index + "]视图变更完成:" + view);
            // 可以继续发请求
            if(curREQMsg != null) {
                curREQMsg.setViewNo(view);
                logger.info("[节点" + index + "]请求重传:" + curREQMsg);
                doSendCurMsg();
            }
        }
    }

    // 执行对应请求
    private void doSomething(PBFTMsg msg) {
        logger.info("[节点" + index + "]成功执行请求" + msg);
    }

    // 请求入列
    public void req(String data) {
        PBFTMsg REQMsg = new PBFTMsg(MessageEnum.REQUEST, index);
        REQMsg.setDataHash(data);
        try {
            reqQueue.put(REQMsg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 检查请求
    private boolean doReq() {
        if(!viewOK || curREQMsg != null) return false; // 视图初始化中/上一个请求还未完成
        curREQMsg = reqQueue.poll();
        if(curREQMsg == null) return false;
        curREQMsg.setViewNo(view);
        doSendCurMsg();
        return true;
    }

    // 发送当前请求消息给主节点
    private void doSendCurMsg() {
        // 记录发送时间, 用于后续判断超时
        REQMsgTimeout.put(curREQMsg.getDataHash(), System.currentTimeMillis());
        // 把当前请求发给主节点
        send(getPrimeIndex(view), curREQMsg);
    }

    // 初始化视图view
    private void pubView() {
        PBFTMsg VIEWMsg = new PBFTMsg(MessageEnum.VIEW,index);
        publish(VIEWMsg);
    }

    private boolean checkMsg(PBFTMsg msg, boolean isPre) {
        return (msg.isValid() && msg.getViewNo() == view
                // pre-prepare阶段校验
                // 如果是自己的消息则无需之后的校验
                // pre-prepare消息必须从主节点收到, 且序列号大于当前序列号
                && (!isPre || msg.getSenderId() == index || (getPrimeIndex(view) == msg.getSenderId() && msg.getSeqNo() > genSeqNo.get())));
    }

    // 检测超时情况
    private void checkTimer() {
        List<String> timeoutList = Lists.newArrayList();
        for(Map.Entry<String, Long> item : PBFTMsgTimeout.entrySet()) {
            if(System.currentTimeMillis() - item.getValue() > timeout) {
                logger.info("投票无效["+index+"](数据哈希+序列号):"+ item.getKey());
                // 超时计时器翻倍
                timeout <<= 1;
                logger.info("[节点" + index + "]超时计时器翻倍");
                timeoutList.add(item.getKey());
            }
        }
        timeoutList.forEach((it) -> cleanCache(it));
        timeoutList.clear();

        for(Map.Entry<String, Long> item : REQMsgTimeout.entrySet()) {
            if(System.currentTimeMillis() - item.getValue() > timeout) {
                timeoutList.add(item.getKey());
            }
        }
        // client请求主节点超时, 进入
        timeoutList.forEach((it) -> {
            logger.info("请求主节点超时[" + index + "]:" + it);
            REQMsgTimeout.remove(it);
            if(curREQMsg != null && curREQMsg.getDataHash().equals(it)) {
                // 作为客户端发起节点
                VCMsgRecord.add(index + "|@|" + (view+1));
                VCMsgCountMap.incrementAndGet(view+1);
                // 广播当前请求
                publish(curREQMsg);
            } else {
                if(!viewOK) return; //已经开始选举视图, 不需要重复发起
                viewOK = false;
                // 作为副本节点, 广播视图变换信息
                PBFTMsg VCMsg = new PBFTMsg(MessageEnum.VIEW_CHANGE, index);
                VCMsg.setViewNo(view+1);
                publish(VCMsg);
            }
        });
    }

    private void cleanCache(String it) {
        PPMsgRecord.remove(it);
        PAMsgRecord.removeIf((vp) -> StringUtils.startsWith(vp, it));
        CMMsgRecord.removeIf((vp) -> StringUtils.startsWith(vp, it));
        PAMsgCountMap.remove(it);
        CMMsgCountMap.remove(it);
        PBFTMsgTimeout.remove(it);
    }

    public int getPrimeIndex(int view) {
        return view % n;
    }

    // 广播消息
    public synchronized void publish(PBFTMsg msg) {
        logger.info("[节点" + index + "]广播消息:" + msg);
        for(int i = 0; i < n; i++) {
            send(i, new PBFTMsg(msg)); // 广播时发送消息的复制
        }
    }

    // 发送消息给指定节点, 加上synchronized按顺序发送
    public synchronized void send(int toIndex, PBFTMsg msg) {
        // 模拟发送时长
        try {
            Thread.sleep(sendMsgTime(msg, BANDWIDTH));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        totalSendMsgLen += msg.getMsgLen();

        // 模拟网络时延
        TimerManager.schedule(() -> {
            PBFTMain.nodes[toIndex].pushMsg(msg);
            return null;
        }, PBFTMain.netDelay[index][toIndex]);
    }

    // 发送消息所耗的时长, 单位ms
    public long sendMsgTime(PBFTMsg msg, int bandwidth) {
        return msg.getMsgLen() * 1000 / bandwidth;
    }

    public void pushMsg(PBFTMsg msg) {
        try {
            qbm.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

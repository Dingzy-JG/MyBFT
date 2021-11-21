package pbft;

import constant.ConstantValue;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.TimerManager;
import java.util.Random;

public class PBFTMain {

    static Logger logger = LoggerFactory.getLogger(PBFTMain.class);

    public static final int size = 100;                             // 节点数量
    private static PBFTNode[] nodes = new PBFTNode[500];            // 节点集合
    private static Random r = new Random();                         // 用于生成随机数
    private static long[][] netDelay = new long[500][500];          // 用随机数代表网络延迟

    public static void main(String[] args) throws InterruptedException {
        for(int i = 0; i < size; i++) {
            nodes[i] = new PBFTNode(i, size).start();
        }

        initNet(ConstantValue.FAST_NET_DELAY, ConstantValue.SLOW_NET_DELAY, ConstantValue.TO_ITSELF_DELAY);

        // 模拟client发送请求
        for(int i = 0; i < 1; i++) {
            int node = r.nextInt(size);
            nodes[node].req("test"+i);
        }
    }

    // 初始化网络延迟
    private static void initNet(Long fast, Long slow, Long toItself) {
        for(int i = 0; i < size; i++) {
            for(int j = 0; j < size; j++) {
                if(i != j) {
                    netDelay[i][j] = RandomUtils.nextLong(fast, slow);
                } else {
                    netDelay[i][j] = toItself;
                }
            }
        }
    }

    // 广播消息
    public static void publish(PBFTMsg msg){
        logger.info("[节点" + msg.getSenderId() + "]广播消息:" + msg);
        for(int i = 0; i < size; i++) {
            final int temp = i;
            TimerManager.schedule(()->{
                nodes[temp].pushMsg(new PBFTMsg(msg));
                return null;
            }, netDelay[msg.getSenderId()][nodes[temp].getIndex()]);
        }
    }

    // 发送消息给指定节点
    public static void send(int toIndex, PBFTMsg msg){
        // 模拟网络时延
        TimerManager.schedule(()-> {
            nodes[toIndex].pushMsg(msg);
            return null;
        }, netDelay[msg.getSenderId()][toIndex]);
    }
}

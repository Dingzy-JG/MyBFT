package pbft;

import org.apache.commons.lang3.RandomUtils;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static constant.ConstantValue.*;

public class PBFTMain {

    public static final int size = NODE_SIZE;                               // 节点数量
    public static final int transactionNum = TRANSACTION_NUMBER;            // 交易的数量 (PBFT过程执行的次数)
    public static PBFTNode[] nodes = new PBFTNode[500];                     // 节点集合
    public static Random r = new Random();                                  // 用于生成随机数
    public static long[][] netDelay = new long[500][500];                   // 用随机数代表网络延迟

    public static double communicationCost = 0;


    // =====================================用于计时=====================================
    public static long startTime, endTime;
    public static CountDownLatch countDownLatch = new CountDownLatch(transactionNum);
    // =====================================用于计时=====================================

    public static void main(String[] args) throws InterruptedException {
        initNet(FAST_NET_DELAY, SLOW_NET_DELAY, TO_ITSELF_DELAY);
        for(int i = 0; i < size; i++) {
            nodes[i] = new PBFTNode(i, size).start();
        }

        // 模拟client发送请求
        for(int i = 0; i < transactionNum; i++) {
            int node = r.nextInt(size);
            nodes[node].req("test"+i);
        }

        // =====================================用于计时=====================================
        countDownLatch.await();
        endTime = System.currentTimeMillis();
        Thread.sleep(1000);
        for(int i = 0; i < size; i++) {
            communicationCost += nodes[i].totalSendMsgLen;
        }
        System.out.println("通信开销为: " + (communicationCost/8/1024) + "KB");
        System.out.println("耗时: " + (endTime-startTime) + "ms");
        // =====================================用于计时=====================================

    }

    // 初始化网络延迟
    private static void initNet(long fast, long slow, long toItself) {
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
}

package bilayer;

import org.apache.commons.lang3.RandomUtils;

import static constant.ConstantValue.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class bilayerBFTMain {

    public static final int size = NODE_SIZE;                                       // 节点数量
    public static final int groupNum = getGroupNum(size);                           // 组数
    public static final int[] groupSizeArr = getGroupSizeArr(size, groupNum);       // 各组中节点数量
    public static final int transactionNum = TRANSACTION_NUMBER;                    // 交易的数量 (PBFT过程执行的次数)
    public static bilayerBFTNode[] nodes = new bilayerBFTNode[550];                 // 节点集合, 取550主要因为当节点数为499时, 有些组有11个
    public static Random r = new Random();                                          // 用于生成随机数
    public static long[][] netDelay = new long[550][550];                           // 用随机数代表网络延迟

    public static double communicationCost = 0;


    // =====================================用于计时=====================================
    public static long startTime, endTime;
    public static CountDownLatch countDownLatch = new CountDownLatch(transactionNum);
    // =====================================用于计时=====================================

    public static void main(String[] args) {
        initNet(GROUP_INSIDE_FAST_NET_DELAY, GROUP_INSIDE_SLOW_NET_DELAY, GROUP_OUTSIDE_FAST_NET_DELAY, GROUP_OUTSIDE_SLOW_NET_DELAY, TO_ITSELF_DELAY);


    }

    // 初始化网络延迟, 先用组间延迟随机填满, 再把组内延迟覆盖更新
    // 节点需要已经做过处理, 每组分配OFFSET个连续的序号, 能被OFFSET整除的序号对应的节点为leader
    private static void initNet(long innerGroupFast, long innerGroupSlow, long outerGroupFast, long outerGroupSlow, long toItself) {
        // 先用组间随机范围填满
        for(int i = 0; i < 550; i++) {
            for(int j = 0; j < 550; j++) {
                netDelay[i][j] = RandomUtils.nextLong(outerGroupFast, outerGroupSlow);
            }
        }
        // 再用组内的覆盖对应的
        for(int c = 0; c < groupNum; c++) {
            for(int i = OFFSET*c; i < OFFSET*(c+1); i++) {
                for(int j = OFFSET*c; j < OFFSET*(c+1); j++) {
                    if(i != j) {
                        netDelay[i][j] = RandomUtils.nextLong(innerGroupFast, innerGroupSlow);
                    } else {
                        netDelay[i][j] = toItself;
                    }
                }
            }
        }
    }

    private static int[] getGroupSizeArr(int nodeSize, int groupNum) {
        int[] res = new int[groupNum];
        int standardSize = nodeSize / groupNum;
        int biggerSize = nodeSize - standardSize * groupNum;
        for(int i = 0; i < biggerSize; i++) {
            res[i] = standardSize + 1;
        }
        for(int i = biggerSize; i < groupNum; i++) {
            res[i] = standardSize;
        }
        return res;
    }

    private static int getGroupNum(int nodeSize) {
        int temp = nodeSize*nodeSize/2;
        int res = (int) Math.pow(temp, 1.0/3);
        // 存在精度损失, 如当nodeSize = 500时
        // (int) Math.pow(125000, 1.0/3) = 49
        res++;
        if(res*res*res <= temp) {
            return res;
        } else {
            return res-1;
        }
    }

}

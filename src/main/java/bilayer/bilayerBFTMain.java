package bilayer;

import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import org.apache.commons.lang3.RandomUtils;

import static constant.ConstantValue.*;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class bilayerBFTMain {

    public static final int size = NODE_SIZE;                                       // 节点数量
    public static final int groupNum = getGroupNum(size);                           // 组数
    public static final int[] groupSizeArr = getGroupSizeArr(size, groupNum);       // 各组中节点数量
    public static final int[] node2Index = getNode2Index();                         // 通过数组nodes下标找index, 初始化节点时给出正确的index
    public static final int[] index2Node = getIndex2Node();                         // 通过index找nodes中的下标, 为了方便查找发送时间
    public static final int transactionNum = TRANSACTION_NUMBER;                    // 交易的数量 (PBFT过程执行的次数)
    public static bilayerBFTNode[] nodes = new bilayerBFTNode[500];                 // 节点集合
    public static Random r = new Random();                                          // 用于生成随机数
    public static long[][] netDelay = new long[550][550];                           // 用随机数代表网络延迟, 下标代表对应index之间的延迟, 取550主要因为当节点数为499时, 有些组有11个

    public static double communicationCost = 0;


    // =====================================用于计时=====================================
    public static long startTime, endTime;
    public static CountDownLatch countDownLatch = new CountDownLatch(transactionNum);
    // =====================================用于计时=====================================

    public static void main(String[] args) throws InterruptedException {
        initNet(GROUP_INSIDE_FAST_NET_DELAY, GROUP_INSIDE_SLOW_NET_DELAY, GROUP_OUTSIDE_FAST_NET_DELAY, GROUP_OUTSIDE_SLOW_NET_DELAY, TO_ITSELF_DELAY);
        for(int i = 0; i < size; i++) {
            int index = node2Index[i];
            nodes[i] = new bilayerBFTNode(index, size, groupSizeArr[getGroupIndex(index)], isLeader(index)).start();
        }

        // 模拟client发送请求
        for(int i = 0; i < transactionNum; i++) {
            int node = r.nextInt(size);

            // =====================================用于计时=====================================
            if(startTime == 0) startTime = System.currentTimeMillis();
            // =====================================用于计时=====================================

            nodes[node].req("test"+i);
        }

        // =====================================用于计时=====================================
        countDownLatch.await();
        endTime = System.currentTimeMillis();
        for(int i = 0; i < size; i++) {
            communicationCost += nodes[i].totalSendMsgLen;
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = formatter.format(startTime);

        String result = "bilayerBFT:\n";
        result += time + '\n';
        result += "节点数量为:" + size + '\n';
        result += "通信开销为: " + (communicationCost/8/1024) + "KB \n";
        result += "耗时: " + (endTime-startTime) + "ms";
        WriteResultToFile("result.txt", result);
        // =====================================用于计时=====================================

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

    // 判断对应index是否为leader
    private static boolean isLeader(int index) {
        if(size < 32) {
            return index == 0;
        } else {
            return (index % OFFSET == 0);
        }
    }

    private static int[] getIndex2Node() {
        int[] res = new int[550];
        for(int i = 0; i < size; i++) {
            res[node2Index[i]] = i;
        }
        return res;
    }

    private static int[] getNode2Index() {
        int[] res = new int[size];
        int idx = 0;
        for(int i = 0; i < groupNum; i++) {
            for(int j = 0; j < groupSizeArr[i]; j++) {
                res[idx++] = OFFSET * i + j;
            }
        }
        return res;
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

    // 根据节点index获取组号
    // 不判断是否小于32的话会在节点数量大于11小于32时数组访问越界
    private static int getGroupIndex(int index) {
        if(size < 32) return 0;
        return index / OFFSET;
    }

    private static int getGroupNum(int nodeSize) {
        // 节点数量小于32时不分组
        if(nodeSize < 32) {
            return 1;
        }
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

    // 把结果输出到文件中
    private static void WriteResultToFile(String filePath, String result) {
        try {
            FileOutputStream fos = new FileOutputStream(filePath);
            fos.write(result.getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

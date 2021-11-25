package bilayer;

import static constant.ConstantValue.*;

import java.util.Random;
import pbft.PBFTNode;

public class bilayerBFTMain {

    public static final int size = NODE_SIZE;                               // 节点数量
    public static final int transactionNum = TRANSACTION_NUMBER;            // 交易的数量 (PBFT过程执行的次数)
    public static bilayerBFTNode[] nodes = new bilayerBFTNode[500];         // 节点集合
    public static Random r = new Random();                                  // 用于生成随机数
    public static long[][] netDelay = new long[500][500];                   // 用随机数代表网络延迟

    public static double communicationCost = 0;

}

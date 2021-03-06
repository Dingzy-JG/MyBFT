package constant;

import bilayer.bilayerBFTMain;

public class ConstantValue {

    // =====================================共用的值=====================================

    public static final int NODE_SIZE = 200;
    public static final double FAULT_RATIO = 1.0/4;
    public static final int FAULT_NODE_SIZE = (int) (NODE_SIZE * FAULT_RATIO);
    public static final int TRANSACTION_NUMBER = 1;
//    public static final int TRANSACTION_SIZE = 1 * 1024 * 1024 * 8; // 1M
    public static final int TOTAL_BANDWIDTH = 50 * 1024 * 8;
    public static final int BANDWIDTH = TOTAL_BANDWIDTH / NODE_SIZE;
    public static final int MSG_TYPE_ID_SIZE = 4; // 用于标识消息类型, 类型不多于16种, 可以标识完全
    public static final int TIMESTAMP_SIZE = 64; //时间戳为long类型, 64位
    public static final int ID_SIZE = 16; // 单位: b
    public static final int HASH_SIZE = 256;
    public static final int SIGNATURE_SIZE = 512;
    public static final int RESULT_SIZE = 1; // 值为0或1, 用于reply中的r和bilayer消息中的b
    public static final long TO_ITSELF_DELAY = 5;

    // =====================================PBFT=====================================

    public static final int VIEW_NO_SIZE = 8;
    public static final int SEQ_NO_SIZE = 16;
    public static final int C_SET_SIZE = 10000; // 表示集合的总共大小 (估计)
    public static final int P_SET_SIZE = 10000;
    public static final long INIT_TIMEOUT = 50000000; // 单位: ms
    public static final long FAST_NET_DELAY = 5;
    public static final long SLOW_NET_DELAY = 30;

    // =====================================bilayer=====================================

    // 分组后一组中最多有多少个节点, 实验最大节点数为500个
    // 最多的情况为499时, 分为49组, 其中有9组为11个节点
    public static final int OFFSET = 11;
    public static final int WEIGHT_SIZE = 16;
    public static final int PK_SIZE = 512;
    public static final long GROUP_INSIDE_FAST_NET_DELAY = 5;
    public static final long GROUP_INSIDE_SLOW_NET_DELAY = 10;
    public static final long GROUP_OUTSIDE_FAST_NET_DELAY = 10;
    public static final long GROUP_OUTSIDE_SLOW_NET_DELAY = 30;

    // 节点数31个时不准
    // 计算理论最长用时
    public static final long SEND_WEIGHT_TIME =
        // 1. 组内广播prepare消息
        (MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + SIGNATURE_SIZE) * 1000 * bilayerBFTMain.groupSizeArr[0] / BANDWIDTH + GROUP_INSIDE_SLOW_NET_DELAY +
        // 2. 组内广播commit消息
        (MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + SIGNATURE_SIZE) * 1000 * bilayerBFTMain.groupSizeArr[0] / BANDWIDTH + GROUP_INSIDE_SLOW_NET_DELAY +
        // 3. 给leader发reply消息 + 最开始的延迟100ms
        (MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + RESULT_SIZE + SIGNATURE_SIZE + SIGNATURE_SIZE) * 1000 / BANDWIDTH + GROUP_INSIDE_SLOW_NET_DELAY + 100;

    public static final long GATHER_NO_BLOCK_TIME =
        // 1. 也算上发送no_reply消息的时长
        (MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + SIGNATURE_SIZE) * 1000 * bilayerBFTMain.groupSizeArr[0] / BANDWIDTH + GROUP_INSIDE_SLOW_NET_DELAY +
        // 2. 组内节点给leader发no_block消息的时长
        (MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + RESULT_SIZE + SIGNATURE_SIZE + SIGNATURE_SIZE) * 1000 / BANDWIDTH + GROUP_INSIDE_SLOW_NET_DELAY;
    public static final long SEND_PROOF_HONEST_TIME = 600000; // (10min)根据实际情况设置

}

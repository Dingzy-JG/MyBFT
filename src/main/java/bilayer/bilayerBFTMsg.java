package bilayer;

import enums.MessageEnum;

public class bilayerBFTMsg {

    private MessageEnum type;                  // 消息类型
    private int senderId;                      // 发送节点的id
    private int primeNodeId;                   // 发起请求的节点id
    private String dataHash;                   // 区块的Hash值, 用于标识不同区块
    private int b;                             // 输入ABA的值
    private long timestamp;                    // 时间戳
    private boolean isValid;                   // 消息校验是否通过

}

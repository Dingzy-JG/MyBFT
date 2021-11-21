package pbft;

import enums.MessageEnum;

public class PBFTMsg {

    private MessageEnum type;                  // 消息类型
    private int senderId;                      // 发送节点的id
    private int primeNodeId;                   // 发起请求的节点id
    private int viewNo;                        // 视图编号
    private int seqNo;                         // 序列号
    private long timestamp;                    // 时间戳
    private String dataHash;                   // 数据的Hash值
    private boolean isValid;                   // 消息校验是否通过

    public PBFTMsg() {
    }

    // 用于生成Request消息, 所以clientNodeId == nodeId [?]
    public PBFTMsg(MessageEnum type, int senderId) {
        this.type = type;
        this.senderId = senderId;
        this.primeNodeId = senderId; // 为什么同nodeId [?]
        this.timestamp = System.currentTimeMillis();
        this.isValid = true;
    }

    // 拷贝
    public PBFTMsg(PBFTMsg msg) {
        this.type = msg.type;
        this.senderId = msg.senderId;
        this.primeNodeId = msg.primeNodeId;
        this.viewNo = msg.viewNo;
        this.seqNo = msg.seqNo;
        this.timestamp = msg.timestamp;
        this.dataHash = msg.dataHash;
         this.isValid = msg.isValid;
    }

    // 用于后面放到Map中里当key
    public String getDataKey() {
        return getDataHash() + "|@|" + getSeqNo();
    }

    public String getKey() {
        return getDataKey() + "|@|" + getSenderId();
    }

    public MessageEnum getType() {
        return type;
    }

    public void setType(MessageEnum type) {
        this.type = type;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public int getPrimeNodeId() {
        return primeNodeId;
    }

    public void setPrimeNodeId(int primeNodeId) {
        this.primeNodeId = primeNodeId;
    }

    public int getViewNo() {
        return viewNo;
    }

    public void setViewNo(int viewNo) {
        this.viewNo = viewNo;
    }

    public int getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(int seqNo) {
        this.seqNo = seqNo;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDataHash() {
        return dataHash;
    }

    public void setDataHash(String dataHash) {
        this.dataHash = dataHash;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    @Override
    public String toString() {
        return "PBFTMsg [" +
                "isValid=" + isValid +
                ", type=" + type +
                ", primeNodeId=" + primeNodeId +
                ", senderId=" + senderId +
                ", viewNo=" + viewNo +
                ", dataHash=" + dataHash +
                ", seqNo=" + seqNo +
                ", timestamp=" + timestamp + "]";
    }
}

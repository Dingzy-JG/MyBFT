package pbft;

import enums.MessageEnum;
// 静态导包, 计算消息长度时不用再加前缀
import static constant.ConstantValue.*;

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

    // 用于生成Request消息, primeNodeId == nodeId. 其他调用时对senderId进行修改
    public PBFTMsg(MessageEnum type, int senderId) {
        this.type = type;
        this.senderId = senderId;
        this.primeNodeId = senderId;
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

    public String getMsgKey() {
        return getDataKey() + "|@|" + getSenderId();
    }

    // 获取消息长度
    public long getMsgLen() {
        long len = 0;
        switch (type) {
            case REQUEST:
                len =  MSG_TYPE_ID_SIZE + HASH_SIZE + TIMESTAMP_SIZE + ID_SIZE + SIGNATURE_SIZE;
                break;
            case PRE_PREPARE:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + SEQ_NO_SIZE + HASH_SIZE + SIGNATURE_SIZE;
                break;
            case PREPARE:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + SEQ_NO_SIZE + HASH_SIZE + ID_SIZE + SIGNATURE_SIZE;
                break;
            case COMMIT:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + SEQ_NO_SIZE + HASH_SIZE + ID_SIZE + SIGNATURE_SIZE;
                break;
            case REPLY:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + TIMESTAMP_SIZE + ID_SIZE + ID_SIZE + RESULT_SIZE + SIGNATURE_SIZE;
                break;
            case VIEW:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + SIGNATURE_SIZE;
                break;
            case VIEW_CHANGE:
                len =  MSG_TYPE_ID_SIZE + VIEW_NO_SIZE + SEQ_NO_SIZE + C_SET_SIZE + P_SET_SIZE + ID_SIZE + SIGNATURE_SIZE;
                break;
            default:
                break;
        }
        return len;
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
        return "PBFTMsg {" +
                "isValid=" + isValid +
                ", seqNo=" + seqNo +
                ", type=" + type +
                ", primeNodeId=" + primeNodeId +
                ", senderId=" + senderId +
                ", viewNo=" + viewNo +
                ", dataHash=" + dataHash +
                ", timestamp=" + timestamp + "}";
    }
}

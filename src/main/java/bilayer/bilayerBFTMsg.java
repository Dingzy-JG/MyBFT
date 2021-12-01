package bilayer;

import enums.MessageEnum;
import java.math.BigInteger;
import static constant.ConstantValue.*;

public class bilayerBFTMsg {

    private MessageEnum type;                  // 消息类型
    private int senderId;                      // 发送节点的id
    private BigInteger senderPK;               // 发送节点pk
    private int primeNodeId;                   // 发起请求的节点id
    private String dataHash;                   // 区块的Hash值, 用于标识不同区块
    private Integer b;                         // 输入ABA的值(0, 1, null)
    private long timestamp;                    // 时间戳
    private BigInteger signature;              // 签名
    private boolean isValid;                   // 消息校验是否通过

    public bilayerBFTMsg() {
    }

    public bilayerBFTMsg(MessageEnum type, int senderId) {
        this.type = type;
        this.senderId = senderId;
        this.primeNodeId = senderId;
        this.timestamp = System.currentTimeMillis();
        this.isValid = true;
    }

    // 拷贝
    public bilayerBFTMsg(bilayerBFTMsg msg) {
        this.type = msg.type;
        this.senderId = msg.senderId;
        this.senderPK = msg.senderPK;
        this.primeNodeId = msg.primeNodeId;
        this.dataHash = msg.dataHash;
        this.b = msg.b;
        this.timestamp = msg.timestamp;
        this.signature = msg.signature;
        this.isValid = msg.isValid;
    }

    // PBFTMsg中用seqNo来处理未成功请求再次发送不被拒绝
    // bilayerBFT中没有seqNo, 用timestamp代替
    public String getDataKey() {
        return getDataHash() + "|@|" + timestamp;
    }

    // 得到消息标识
    public String getMsgKey() {
        return getDataKey() + "|@|" + getSenderId();
    }

    // 获取消息长度
    public long getMsgLen() {
        long len = 0;
        switch (type) {
            case REQUEST:
                len =  MSG_TYPE_ID_SIZE + HASH_SIZE + TIMESTAMP_SIZE + ID_SIZE + PK_SIZE + SIGNATURE_SIZE;
                break;
            case PREPARE:
            case COMMIT:
            case WEIGHT:
            case NO_REPLY:
            case PROOF_HONEST:
                len =  MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + SIGNATURE_SIZE;
                break;
            case REPLY:
            case NO_BLOCK:
                len =  MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + RESULT_SIZE + SIGNATURE_SIZE + SIGNATURE_SIZE;
                break;
            case AFFIRM_HONEST:
                len = MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + SIGNATURE_SIZE + SIGNATURE_SIZE;
                break;
            case WABA:
                len = MSG_TYPE_ID_SIZE + ID_SIZE + PK_SIZE + HASH_SIZE + WEIGHT_SIZE + RESULT_SIZE + SIGNATURE_SIZE;
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

    public BigInteger getSenderPK() {
        return senderPK;
    }

    public void setSenderPK(BigInteger senderPK) {
        this.senderPK = senderPK;
    }

    public int getPrimeNodeId() {
        return primeNodeId;
    }

    public void setPrimeNodeId(int primeNodeId) {
        this.primeNodeId = primeNodeId;
    }

    public String getDataHash() {
        return dataHash;
    }

    public void setDataHash(String dataHash) {
        this.dataHash = dataHash;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public BigInteger getSignature() {
        return signature;
    }

    public void setSignature(BigInteger signature) {
        this.signature = signature;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    @Override
    public String toString() {
        return "bilayerBFTMsg {" +
            "isValid=" + isValid +
            ", type=" + type +
            ", primeNodeId=" + primeNodeId +
            ", senderId=" + senderId +
            ", senderPK=" + senderPK +
            ", dataHash=" + dataHash +
            ", b=" + b +
            ", timestamp=" + timestamp +
            ", signature=" + signature + '}';
    }
}

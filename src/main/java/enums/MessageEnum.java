package enums;

public enum MessageEnum {
    VIEW_CHANGE,                  // 视图变更
    VIEW,                         // 请求视图
    REQUEST,                      // 请求
    PRE_PREPARE,                  // 预准备阶段
    PREPARE,                      // 准备阶段
    COMMIT,                       // 提交阶段
    REPLY,                        // 回复
    WEIGHT,                       // 权重计算
    WABA,                         // WABA阶段的消息
    AUX,                          // WABA中的AUX
    WABA_RESULT,                  // WABA的结果
    NO_REPLY,                     // leader发现无REPLY向组员收集对应为0的签名
    NO_BLOCK,                     // 收到leader的NO_BLOCK, 确实自己没有收到后发回给leader的消息
    PROOF_HONEST,                 // client太久未收到区块提交的信息, 收集签名证明自己诚实
    AFFIRM_HONEST                 // 其他节点确认自己收到了对应消息, 证明其诚实的消息
}

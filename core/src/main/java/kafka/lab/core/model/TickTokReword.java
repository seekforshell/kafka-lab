package kafka.lab.core.model;

import lombok.Data;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
@Data
public class TickTokReword {
	// 直播Id
	String live_id;
	// 粉丝Id
	String fans_id ;
	// 礼物类型
	String reward_gift_id;
	// 礼物数量
    Integer reward_gift_num;
    // 打赏时间
    String reward_time;
}

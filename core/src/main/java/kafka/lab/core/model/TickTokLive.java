package kafka.lab.core.model;

import lombok.Data;

/**
 * @author: yujingzhi
 * Version: 1.0
 */
@Data
public class TickTokLive {
	// 直播Id
	String live_id;
	// 博主Id
	String blogger_id;
	// 直播时间
	String live_time;
}

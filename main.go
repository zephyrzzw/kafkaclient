package kafkaclient

import "log"
import "context"
import "github.com/IBM/sarama"

// 生产者
func ProducerMsg(broders []string, topic string, clientid string, MSG *chan []byte) {
	config := sarama.NewConfig()
	if clientid != "" {
		config.ClientID = clientid
	}
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follow都确认
	config.Producer.Flush.Frequency = 500 * 1000000  // 500ms
	config.Producer.Return.Successes = true          // 成功交付的消息将在success channel返回
	config.Producer.Return.Errors = true             // 失败返回错误
	config.Producer.Retry.Max = 10                   // 重试次数
	config.Producer.Return.Successes = true          // 成功交付的消息将在success channel返回
	producer, err := sarama.NewSyncProducer(broders, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}
	defer producer.Close()
	for {
		select {
		case msg := <-*MSG: // 读取消息
			Msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(msg),
			}
			if _, _, err := producer.SendMessage(Msg); err != nil {
				log.Println("Failed to write message:", err)
			}
		}
	}
}

// 消费者
func ConsumerMsg(broders []string, topic string, groupid string, clientid string, MSG *chan []byte) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true                  // 启用错误返回
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // 默认从最新消息开始消费
	if clientid != "" {
		config.ClientID = clientid
	}
	group, err := sarama.NewConsumerGroup(broders, groupid, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama consumer:", err)
	}
	defer group.Close()
	go func() {
		for err := range group.Errors() {
			log.Println("Error from consumer:", err)
		}
	}()

	var handler = &MyConsumerGroupHandler{
		Msg: MSG,
	}
	// 消费
	for {
		if err := group.Consume(context.Background(), []string{topic}, handler); err != nil {
			log.Fatalln("Error from consumer:", err)
		}
	}

}

// MyConsumerGroupHandler 是 ConsumerGroupHandler 接口的实现
type MyConsumerGroupHandler struct {
	Msg *chan []byte
}

// Setup 执行会话开始前的初始化操作
func (h *MyConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// 初始化代码
	return nil
}

// Cleanup 执行会话结束后的清理操作
func (h *MyConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	// 清理代码
	return nil
}

// ConsumeClaim 处理分配给消费者的消息
func (h *MyConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 处理消息
		*h.Msg <- message.Value
		// 确认消息已经被处理
		session.MarkMessage(message, "")
	}
	return nil
}

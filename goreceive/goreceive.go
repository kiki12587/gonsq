//Nsq消费者测试
package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"time"
)

// 消费者
type ConsumerT struct{}

// 主函数
func main() {
	//这是我的服务器nsqd发送和消费的端口 这是我就演示只建立跟一个nsqd的连接
	InitConsumer("test", "test-channel", "134.175.138.178:4150")
	for {
		time.Sleep(time.Second * 10)
	}
}

//处理消息
func (*ConsumerT) HandleMessage(msg *nsq.Message) error {
	fmt.Println("receive", msg.NSQDAddress, "message:", string(msg.Body))
	return nil
}

//初始化消费者
func InitConsumer(topic string, channel string, address string) {
	cfg := nsq.NewConfig()
	cfg.LookupdPollInterval = time.Second          //设置重连时间
	c, err := nsq.NewConsumer(topic, channel, cfg) // 新建一个消费者
	if err != nil {
		panic(err)
	}
	c.SetLogger(nil, 0)        //屏蔽系统日志
	c.AddHandler(&ConsumerT{}) // 添加消费者接口

	//建立NSQLookupd连接
	//if err := c.ConnectToNSQLookupd(address); err != nil {
	//	panic(err)
	//}

	//建立多个nsqd连接
	//if err := c.ConnectToNSQDs([]string{"134.175.138.178:4150", "134.175.138.178:4151"}); err != nil {
	//panic(err)
	//}

	// 建立一个nsqd连接
	if err := c.ConnectToNSQD(address); err != nil {
	panic(err)
	}
}
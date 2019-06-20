package main

import (
	"go-rabbit/rabbit"
)

/*
	支持断线重连
	失败重发
    @author : Bill
*/
func main() {
	var(
		addr = "amqp://guest:guest@localhost:5672/"
		queue = "testQueue"
		exchange = "test_exchange"
		routerKey = "/test"
		msg = "test1!"

		//delay
		delayQueue = "delay_queue"
		delayExchange = "delay_exchange"
		delayRouterKey = "delay_exchange"
		prefix = "v1_prefix"
		sep = "_"
		eType = "F"
		_ttl = 60 * 1000
	)

	var rabbitProduct1 = rabbit.NewRabbitProduct(addr,_ttl,prefix,sep,delayExchange,delayQueue,delayRouterKey)
	//注册回收
	go rabbitProduct1.InitDefdelay(false)
	go rabbitProduct1.InitDefdelay(true)
	go rabbitProduct1.RegisterDelayWithPreFix("delay_queue","delay_exchange","delay_exchange")

	// ttl 为  设置死信回收时间  >0 则回收
	rabbitProduct1.PubMessage(true,eType,queue,exchange,routerKey,msg,rabbitProduct1.GetBool(1),rabbitProduct1.GetBool(0),_ttl)

}
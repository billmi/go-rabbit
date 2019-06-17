package main

import (
	"log"
	"go-rabbit/rabbit"
	"time"
	"fmt"
)

/*
	实现断线重连
	失败重发
	confirm可以同步设置
	可以根据api做后期扩展
	TODO :: channel池
    @author : Bill
*/
func main() {
	var(
		addr = "amqp://guest:guest@localhost:5672/"
		ttl  = 60 * 1000  // 60s回收
		queue = "testQueue"
		exchange = "test_exchange"
		routerKey = "/test"
		msg = "test1!"

		//delay
		delayQueue = "delay_queue"
		delayExchange = "delay_exchange"
		delayRouterKey = "delay_exchange"
		prefix = "v3_comm"
		sep = "_"
		eType = "D"
	)
	//无重发机制 Pub
	var rabbitProduct1 = rabbit.NewRabbitProduct(addr,ttl,prefix,sep,delayExchange,delayQueue,delayRouterKey)
	go rabbitProduct1.InitDefdelay(false)
	go rabbitProduct1.InitDefdelay(true)
	go rabbitProduct1.RegisterDelayWithPreFix("delay_queue","delay_exchange","delay_exchange")
	
	rabbitProduct1.PuBMessage(false,eType,queue,exchange,routerKey,msg,rabbitProduct1.GetBool(1),rabbitProduct1.GetBool(0))
	rabbitProduct1.PuBMessage(true,eType,queue,exchange,routerKey,msg,rabbitProduct1.GetBool(1),rabbitProduct1.GetBool(0))


	//这里时间注意为毫秒 (有重发机制可以根据配置修改)
	var rabbitProduct = rabbit.NewRabbitProduct(addr,ttl,prefix,sep,delayExchange,delayQueue,delayRouterKey)
	go rabbitProduct.InitDefdelay(false)
	go rabbitProduct.InitDefdelay(true)
	for i := 0 ; i < 100 ;i++{
		rabbitProduct.PuB(true,eType,queue,exchange,routerKey,"1214324234234235!",rabbitProduct.GetBool(1),rabbitProduct.GetBool(0))
		rabbitProduct.PuB(false,eType,queue,exchange,routerKey,"1214324234234235!",rabbitProduct.GetBool(1),rabbitProduct.GetBool(0))
		time.Sleep(1*time.Second)
		fmt.Print(i)
		fmt.Print("\r\n")
	}

	log.Println(" end !", )
}
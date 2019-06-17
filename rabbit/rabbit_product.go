package rabbit

import (
	"log"
	"github.com/streadway/amqp"
	"time"
	"os"
	"errors"
)


/**
	构件支持断线重连,与失败重发机制
	Rabbit
	author : Bill
 */

const (
	reconnectDelay = 2 * time.Second  // 连接断开后多久重连
	resendDelay = 5 * time.Second     // 消息发送失败后，多久重发
	resendTime = 3    // 消息重发次数
	maxWaitNum = 5    //最大等待次数
	defSep = "_"
)

var (
	errNotConnected  = errors.New("not connected to the producer")
	errAlreadyClosed = errors.New("already closed: not connected to the producer")
)

type RabbitProduct struct {
	Connection    	*amqp.Connection
	Channel       	*amqp.Channel
	Ttl           	int
	isConnected   	bool
	done          	chan bool
	notifyClose   	chan *amqp.Error
	notifyConfirm 	chan amqp.Confirmation
	logger          *log.Logger
	DelayExchange   string
	DelayQueue      string
	DelayRoute      string
	PreFix          string
	Sep             string
}

//实体
func NewRabbitProduct(addr string,ttl int,prefix string,sep string,delayExchange string,delayQueue string,delayRoute string) *RabbitProduct {
	rabbitProduct := RabbitProduct{
		logger: log.New(os.Stdout, "", log.LstdFlags),
		done:   make(chan bool),
	}
	rabbitProduct.Sep = sep
	if sep == ""{
		rabbitProduct.Sep = defSep
	}
	rabbitProduct.PreFix = prefix
	rabbitProduct.DelayExchange = delayExchange
	rabbitProduct.DelayQueue = delayQueue
	rabbitProduct.DelayRoute = delayRoute
	go rabbitProduct.handleReconnect(addr,ttl)
	return &rabbitProduct
}

//失败重连
func (p *RabbitProduct) handleReconnect(addr string,ttl int) {
	for {
		if p.isConnected == false{
			log.Println("Attempting to connect")
			if ok,_  := p.Conn(addr,ttl); !ok  {
				log.Println("Failed to connect. Retrying...")
				time.Sleep(reconnectDelay)
			}
		}
		select {
		case <-p.done:
			return
		case <-p.notifyClose:
		}
	}
}

//连接
func (p *RabbitProduct)Conn(addr string,ttl int) (bool,error){
	conn,err := amqp.Dial(addr)
	if err != nil{
		return false,err
	}
	p.Connection = conn
	channel,err := conn.Channel()
	channel.Confirm(false)   //为false  true阻塞
	if err != nil{
		return false,err
	}
	p.Ttl = ttl
	p.changeConnection(conn,channel)
	p.isConnected = true
	log.Println("Connected!")
	return true,nil
}

// 监听Rabbit channel的状态
func (p *RabbitProduct) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	p.Connection = connection
	p.Channel = channel

	p.notifyClose = make(chan *amqp.Error)
	p.notifyConfirm = make(chan amqp.Confirmation)
	p.Channel.NotifyClose(p.notifyClose)
	p.Channel.NotifyPublish(p.notifyConfirm)
}

func (p *RabbitProduct)SetTtl(ttl int){
	p.Ttl = ttl
}

func (p *RabbitProduct)GetTtl()int{
	return p.Ttl
}

func (p *RabbitProduct)GetBool(sign int)bool{
	if sign == 0{
		return false
	}
	return true
}

func (p *RabbitProduct)GetType(tTag string)string{
	var _t = ""
	switch tTag {
	case "H":
		_t = amqp.ExchangeHeaders
		break
	case "F":
		_t = amqp.ExchangeFanout
		break
	case "T":
		_t = amqp.ExchangeTopic
		break
	case "D":
		_t = amqp.ExchangeDirect
		break
	default:
		_t = amqp.ExchangeDirect
	}
	return _t
}


//推送消息confirm(有重发机制)
func (p *RabbitProduct)PuB(usePreFix bool,eType string,queue string,exchange string,route string,msg string,durable bool,autoDelete bool)(bool,error){
	if ok,err := p.waitConn();!ok{
		return ok,err
	}
	var prefix = ""
	if usePreFix{
		prefix = p.PreFix + p.Sep
	}
	var currentTime = 0
	var(
		channel = p.Channel
		defType = p.GetType(eType)
		err error
		table = amqp.Table{
			"x-dead-letter-exchange" : prefix + p.DelayExchange,
			"x-dead-letter-routing-key" : prefix + p.DelayRoute,
			"x-message-ttl" : p.Ttl,
		}
		_Exchage = prefix + exchange
		_Queue = prefix + queue
		_Route = prefix + route
	)
	err = channel.ExchangeDeclare(_Exchage,defType, durable, autoDelete, false, false, nil)
	if err != nil{
		return false,err
	}
	_, err = channel.QueueDeclare(_Queue, durable, autoDelete, false, false, table)
	if err != nil{
		return false,err
	}
	err = channel.QueueBind(_Queue, _Route, _Exchage, false, nil)
	if err != nil{
		return false,err
	}
	for {
		err := p.UnsafePush(_Exchage,_Route,msg)
		if err != nil {
			p.logger.Println("Push failed. Retrying...")
			currentTime += 1
			if currentTime < resendTime {
				continue
			}else {
				return false,err
			}
		}
		ticker := time.NewTicker(resendDelay)
		select {
		case confirm := <-p.notifyConfirm:
			if confirm.Ack {
				p.logger.Println("Push confirmed!")
				p.logger.Println(confirm.DeliveryTag)
				p.logger.Println(confirm.Ack)
				return false,nil
			}
		case <- ticker.C:
		}
		p.logger.Println("Push didn't confirm. Retrying...")
	}
}

// 发送出去，不管是否接受的到
func (p *RabbitProduct) UnsafePush(exchange string,routeKey string,data string) error {
	if ok,err := p.waitConn();!ok{
		return err
	}
	return p.Channel.Publish(exchange, routeKey, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
			DeliveryMode:amqp.Persistent,
			Timestamp:  time.Now(),
		},
	)
}

//推送消息
func (p *RabbitProduct)PuBMessage(usePreFix bool,eType string,queue string,exchange string,route string,msg string,durable bool,autoDelete bool)(bool,error){
	if ok,err := p.waitConn();!ok{
		return ok,err
	}
	var prefix = ""
	if usePreFix{
		prefix = p.PreFix + p.Sep
	}
	var(
		channel = p.Channel
		defType = p.GetType(eType)
		err error
		table = amqp.Table{
			"x-dead-letter-exchange" : prefix + p.DelayExchange,
			"x-dead-letter-routing-key" : prefix + p.DelayRoute,
			"x-message-ttl" : p.Ttl,
		}
		_Exchage = prefix + exchange
		_Queue = prefix + queue
		_Route = prefix + route
	)
	err = channel.ExchangeDeclare(_Exchage,defType, durable, autoDelete, false, false, nil)
	if err != nil{
		return false,err
	}
	_, err = channel.QueueDeclare(_Queue, durable, autoDelete, false, false, table)
	if err != nil{
		return false,err
	}
	err = channel.QueueBind(_Queue, _Route, _Exchage, false, nil)
	if err != nil{
		return false,err
	}
	err = channel.Publish(_Exchage, _Route, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			DeliveryMode:amqp.Persistent,
			Timestamp:  time.Now(),
		})
	return true,nil
}

//等待重连
func (p *RabbitProduct)waitConn()(bool,error){
	var _flag = false
	var i = 1
Loop:
	for {
		if i == maxWaitNum{
			break Loop
		}
		if _flag != p.isConnected{
			_flag = true
			break Loop
		}
		i++
		time.Sleep(1 * time.Second)
	}
	if !p.isConnected {
		return _flag,errNotConnected
	}
	return _flag,nil
}

//回收队列注册
func (p *RabbitProduct)RegisterDelay(delayQueue string,delayExchange string,routeKey string) (bool,error){
	if ok,err := p.waitConn();!ok{
		return ok,err
	}
	var(
		err error
		Queue string
		Ex string
		Route string
	)
	if delayQueue != ""{
		Queue = delayQueue
	}
	if delayExchange != ""{
		Ex = delayExchange
	}
	if routeKey != ""{
		Route =  routeKey
	}
	err = p.Channel.ExchangeDeclare(Ex, amqp.ExchangeDirect, true, true, false, false, nil)
	if err != nil{
		return false,err
	}
	_,err = p.Channel.QueueDeclare(Queue , true, false, false, false, nil)
	if err != nil{
		return false,err
	}
	err = p.Channel.QueueBind(Queue,Route,Ex,false,nil)
	if err != nil{
		return false,err
	}
	return true,nil
}

//默认回收队列注册
func (p *RabbitProduct)InitDefdelay(usePreFix bool) (bool,error){
	var prefix = ""
	if usePreFix{
		prefix = p.PreFix + p.Sep
	}
	var (
		DelayQueue = prefix + p.DelayQueue
		DelayExchange = prefix + p.DelayExchange
		DelayRoute = prefix + p.DelayRoute
	)
	return p.RegisterDelay(DelayQueue,DelayExchange,DelayRoute)
}


//外部调用
func (p *RabbitProduct)RegisterDelayWithPreFix(delayQueue string,delayExchange string,routeKey string) (bool,error){
	var (
		pre = p.PreFix + p.Sep
		DelayQueue = pre + delayQueue
		DelayExchange = pre + delayExchange
		route = pre + routeKey
	)
	return p.RegisterDelay(DelayQueue,DelayExchange,route)
}

// 关闭连接
func (p *RabbitProduct) Close() error {
	if !p.isConnected {
		return errAlreadyClosed
	}
	err := p.Channel.Close()
	if err != nil {
		return err
	}
	err = p.Connection.Close()
	if err != nil {
		return err
	}
	close(p.done)
	p.isConnected = false
	return nil
}
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
	Rabbit *RabbitProduct
	errNotConnected  = errors.New("Not connected to the producer")
	errAlreadyClosed = errors.New("Already closed: not connected to the producer")
)

type RabbitProduct struct {
	Addr            string
	Connection    	*amqp.Connection
	Channel       	*amqp.Channel
	Ttl           	int
	isConnected   	bool
	done          	chan bool
	notifyClose   	chan *amqp.Error
	//notifyConfirm 	chan amqp.Confirmation
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
	rabbitProduct.Addr = addr
	go rabbitProduct.handleReconnect(addr,ttl)
	return &rabbitProduct
}


func (p *RabbitProduct) handleReconnect(addr string,ttl int) {
	go func(){
		select {
		case <-p.done:
			return
		case <-p.notifyClose:
			p.Channel.Close()
			p.Connection.Close()
			p.isConnected = false
		}
	}()
	for {
		if  p.isConnected == false || p.Connection.IsClosed() {
			log.Println("Amqp to connect")
			if ok,_  := p.Conn(addr,ttl); !ok  {
				log.Println("Failed to connect. Retrying...")
			}
		}
		time.Sleep(reconnectDelay)
	}
}


func (p *RabbitProduct)Conn(addr string,ttl int) (bool,error){
	conn,err := amqp.Dial(addr)
	if err != nil{
		return false,err
	}
	p.Connection = conn
	channel,err := conn.Channel()
	if err != nil{
		return false,err
	}
	p.Ttl = ttl
	p.changeConnection(conn,channel)
	p.isConnected = true
	log.Println("Connected!")
	return true,nil
}

func (p *RabbitProduct) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	p.Connection = connection
	p.Channel = channel

	p.notifyClose = make(chan *amqp.Error)
	p.Channel.NotifyClose(p.notifyClose)
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
		_t = amqp.ExchangeFanout
	}
	return _t
}


//推送消息
func (p *RabbitProduct)PubMessage(usePreFix bool,eType string,queue string,exchange string,route string,msg string,durable bool,autoDelete bool,ttl int)(bool,error){
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

		_Exchage = prefix + exchange
		_Queue = prefix + queue
		_Route = prefix + route
	)
	if queue == ""{
		_Queue = ""
	}
	if route == ""{
		_Route = ""
	}
	err = channel.ExchangeDeclare(_Exchage,defType, durable, autoDelete, false, false, nil)
	if err != nil{
		return false,err
	}

	err = p.QueueDeclareDelay(_Queue,durable,autoDelete,ttl)
	if err != nil{
		return false,err
	}

	err = channel.QueueBind(_Queue, _Route, _Exchage, false, nil)

	if err != nil{
		return false,err
	}

	err = p.UnsafePush(_Exchage,_Route,msg)
	if err != nil{
		var currentTime = 0
		for {
			err := p.UnsafePush(_Exchage,_Route,msg)
			if err != nil {
				p.logger.Println("Push failed. Retrying...")
				currentTime += 1
				if currentTime < resendTime {
					continue
				}else {
					return false,err
					break //safe
				}
			}
			time.Sleep(resendDelay)
		}
		return false,err
	}
	return true,nil
}

//设置队列回收
func (p *RabbitProduct)QueueDeclareDelay(queue string,durable bool,auto_delete bool,ttl int) error{
	if ok,err := p.waitConn();!ok{
		return err
	}
	var (
		_ttl = p.Ttl
		channel = p.Channel
		err error
	)
	if ttl > 0{
		if _ttl > 0{
			_ttl = ttl
		}
		var table = amqp.Table{
			"x-dead-letter-exchange" : p.PreFix + p.DelayExchange,
			"x-dead-letter-routing-key" : p.PreFix + p.DelayRoute,
			"x-message-ttl" : _ttl,
		}
		_, err = channel.QueueDeclare(queue, durable, auto_delete, false, false, table)
	}else{
		_, err = channel.QueueDeclare(queue, durable, auto_delete, false, false, nil)
	}
	return err
}

//删除队列
func (p *RabbitProduct)DeleteQueue(usePrefix bool,queueName string,use bool,empty bool)(int,error){
	if ok,err := p.waitConn();!ok{
		return 0,err
	}
	_queue_name := queueName
	if usePrefix{
		_queue_name = p.PreFix + p.Sep + _queue_name
	}
	return p.Channel.QueueDelete(_queue_name,use,empty,false)
}

//删除交换机(暂时不提供)
func (p *RabbitProduct)DeleteExchange(usePrefix bool,exchangeName string,use bool,empty bool)(error){
	if ok,err := p.waitConn();!ok{
		return err
	}
	_exchange_name := exchangeName
	if usePrefix{
		_exchange_name = p.PreFix + p.Sep + _exchange_name
	}
	return p.Channel.ExchangeDelete(_exchange_name,use,false)
}

//解绑
func (p *RabbitProduct)QueueUnbind(usePrefix bool,name string,route string,exchange string)error{
	if ok,err := p.waitConn();!ok{
		return err
	}
	_name := name
	_route := route
	_exchange := exchange
	if usePrefix{
		_name     =  p.PreFix + p.Sep + _name
		_route    =  p.PreFix + p.Sep + _route
		_exchange =  p.PreFix + p.Sep + _exchange
	}
	return p.Channel.QueueUnbind(_name,_route,_exchange,nil)
}

//Push
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

//等待重连
func (p *RabbitProduct)waitConn()(bool,error){
	var _flag = false
	var i = 1
Loop:
	for {
		if i == maxWaitNum{
			break Loop
		}
		if _flag != p.isConnected {
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
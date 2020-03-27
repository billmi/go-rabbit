baseFrom : amqp

##### 1. get amqp
`
go get -u -v github.com/streadway/amqp
`

###### 实现断线重连,重发机制,使用前建议简单读下代码!!!

<br/>

###### 主要功能以下(其他后期可自行扩展)

######  --------PubMessage 支持 direct fanout topic headers 模式

###### --------InitDefdelay 注册延迟回收


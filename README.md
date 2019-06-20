baseFrom : amqp

##### 1. get amqp
`
go get -u -v github.com/streadway/amqp
`

#### 实现断线重连,重发机制,只实现Pub接口(后期接口可自行扩展)
<br/>
#### 后期再加入其它工作模型
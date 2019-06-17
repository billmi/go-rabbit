baseFrom : amqp

##### 1. get amqp
`
go get -u -v github.com/streadway/amqp
`

#### 实现断线重连,重发机制,Confirm可以设置为同步(性能会降低),只实现Pub接口
<br/>
#### 后期再加入其它工作模型
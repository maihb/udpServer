package udp_server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/maihb/taskpool"
	log "github.com/sirupsen/logrus"
)

const (
	udp_max_len int = 1472 * 100
)

type udp_recv struct {
	data []byte
	rw   ResponseWriter
	cb   func([]byte, ResponseWriter) //对象内部回调函数，启动服务后不能修改
}

func (u *udp_recv) Do() {
	if u.data == nil || u.rw == nil || u.cb == nil {
		return
	}
	u.cb(u.data, u.rw)
}

type response struct {
	from     *net.UDPAddr
	cb_reply func(data []byte, to *net.UDPAddr) error
}

func (self *response) GetRemoteIp() string {
	if self.from == nil {
		return ""
	}
	return self.from.IP.String()
}
func (self *response) GetRemotePort() int {
	if self.from == nil {
		return 0
	}
	return self.from.Port
}
func (self *response) Reply(data []byte) error {
	if self.cb_reply == nil {
		return fmt.Errorf("cb_reply is null")
	}

	self.cb_reply(data, self.from)
	return nil
}
func (self *response) GetProtocol() string {
	return UDP_PROTOCOL
}
func (self *response) GetConnSession() string {
	//加个特殊前缀，避免和tcp的冲突
	return "udp-client:" + self.from.String()
}
func (self *response) Close() {
	return
}

type UdpInstance struct {
	Port int //监听端口
	p    int //对象内部端口，启动服务后不能修改

	Handler func([]byte, ResponseWriter) //回调函数
	cb      func([]byte, ResponseWriter) //对象内部回调函数，启动服务后不能修改

	conn         *net.UDPConn
	read_queue   []chan *udp_recv
	reader_num   int
	reader_mutex sync.RWMutex

	rtp taskpool.ITaskPool //读包任务池

	reply_num   int
	reply_mutex sync.Mutex
	recv_num    int
	recv_mutex  sync.Mutex
}

func (self *UdpInstance) Serve() {
	if self.Handler == nil {
		log.Println("UdpInstance: Handler is null")
		return
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: self.Port})
	if err != nil {
		log.Fatal(err)
	}
	self.p = self.Port
	self.cb = self.Handler
	log.Printf("begin, ListenAndServe  udp_port:%d", self.p)

	conn.SetReadBuffer(udp_max_len * 100)
	conn.SetWriteBuffer(udp_max_len * 100)
	self.conn = conn

	self.rtp = taskpool.New("UDP_reader",
		taskpool.WithChNum(8))

	go self.producing()
	self.summarize()
}

func (self *UdpInstance) summarize() {
	for {
		sync := 60 - (time.Now().Unix() % 60)
		time.Sleep(time.Second * time.Duration(sync))

		self.reply_mutex.Lock()
		rep := self.reply_num
		self.reply_num = 0
		self.reply_mutex.Unlock()

		self.recv_mutex.Lock()
		recv := self.recv_num
		self.recv_num = 0
		self.recv_mutex.Unlock()

		log.Printf("%d summarize: reply=%d, recv=%d", self.p, rep, recv)
	}
}

func (self *UdpInstance) producing() {
	data := make([]byte, udp_max_len)
	for {
		datalen, from, err := self.conn.ReadFromUDP(data)
		if err != nil {
			log.Println("Error conn.Read:", err)
			continue
		}
		body := make([]byte, datalen)
		copy(body, data)
		self.recv_mutex.Lock()
		self.recv_num++
		self.recv_mutex.Unlock()
		ur := &udp_recv{
			data: body,
			rw:   &response{from: from, cb_reply: self.reply},
			cb:   self.cb,
		}
		self.rtp.Push(ur)
	}
}

func (self *UdpInstance) reply(data []byte, to *net.UDPAddr) error {
	num, err := self.conn.WriteToUDP(data, to)
	if err != nil {
		log.Printf("writeToUDP num=%d err:%v", num, err)
	}
	self.reply_mutex.Lock()
	self.reply_num++
	self.reply_mutex.Unlock()
	return err
}

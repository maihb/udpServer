package udp_server

import (
	"log"
	"testing"
)

func TestUdp(t *testing.T) {
	uServer := UdpInstance{Port: 1234, Handler: UdpComsumer}
	uServer.Serve()
}

func UdpComsumer(body []byte, rw ResponseWriter) {
	log.Println(rw.GetRemoteIp(), string(body))
	rw.Reply(body)
}

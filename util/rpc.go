package util

import (
	"net/rpc"
	"github.com/pkg/errors"
	logr "github.com/sirupsen/logrus"
	"net"
	"github.com/felixge/tcpkeepalive"
	"time"
)

func RpcCall(serverAddress, service string, request interface{}, reply interface{}) (e error) {
	logr.Debugf("rpc call start, server: %s, service: %s", serverAddress, service)
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to connect rpc server: %s", serverAddress)
	}
	err = tcpkeepalive.SetKeepAlive(conn, time.Second*60, 2048, time.Second*45)
	if err != nil {
		return errors.Wrapf(err, "failed to set tcp keep-alive for connection to %s", serverAddress)
	}
	client := rpc.NewClient(conn)
	err = client.Call(service, request, reply)
	if err != nil {
		return errors.Wrapf(err, "rpc service error: %s", service)
	}
	return nil
}

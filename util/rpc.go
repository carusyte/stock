package util

import (
	"net/rpc"
	"github.com/pkg/errors"
)

func RpcCall(serverAddress, service string, request interface{}, reply interface{}) ( e error) {
	client, err := rpc.DialHTTP("tcp", serverAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to connect rpc server: %s", serverAddress)
	}
	err = client.Call(service, request, reply)
	if err != nil {
		return errors.Wrapf(err, "rpc service error: %s", service)
	}
	return nil
}

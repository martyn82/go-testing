package socket

import (
    "net"
    "os"
)

func StartFakeServer(protocol string, address string) net.Listener {
    listener, err := net.Listen(protocol, address)

    if err != nil {
        panic(err)
    }

    return listener
}

func RemoveSocket(socketFile string) {
    os.Remove(socketFile)
}

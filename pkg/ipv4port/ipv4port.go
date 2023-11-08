package ipv4port

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"

	"github.com/tangledbytes/go-vsr/pkg/utils"
)

// IPv4Port is a pair of IPv4 address and port.
type IPv4Port utils.Pair[net.IP, int]

func (ipPort IPv4Port) GetIP() net.IP {
	return ipPort.First
}

func (ipPort IPv4Port) GetPort() int {
	return ipPort.Second
}

func (ipPort *IPv4Port) SetIP(ip net.IP) {
	ipPort.First = ip
}

func (ipPort *IPv4Port) SetPort(port int) {
	ipPort.Second = port
}

func (ipPort IPv4Port) String() string {
	return fmt.Sprintf("%s:%d", ipPort.First.String(), ipPort.Second)
}

func (ipPort *IPv4Port) FromHostPort(hostport string) error {
	ip, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return err
	}

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return fmt.Errorf("invalid IP address: %s", ip)
	}

	if parsedIP.To4() == nil {
		return fmt.Errorf("only IPv4 addresses are supported")
	}

	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	ipPort.SetIP(parsedIP)
	ipPort.SetPort(parsedPort)

	return nil
}

func (ipPort IPv4Port) Less(other IPv4Port) bool {
	if ipPort.GetIP().Equal(other.GetIP()) {
		return ipPort.GetPort() < other.GetPort()
	}

	curr := ipPort.GetIP().To4()
	otherIP := other.GetIP().To4()
	if otherIP == nil {
		panic("other IP is not IPv4")
	}

	parsedCurr := binary.BigEndian.Uint16(curr)
	parsedOther := binary.BigEndian.Uint16(otherIP)

	return parsedCurr < parsedOther
}

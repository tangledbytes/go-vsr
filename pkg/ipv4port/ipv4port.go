package ipv4port

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
)

// IPv4Port is a pair of IPv4 address and port.
type IPv4Port struct {
	First  net.IP
	Second int

	str string
}

func (ipPort IPv4Port) GetIP() net.IP {
	return ipPort.First
}

func (ipPort IPv4Port) GetPort() int {
	return ipPort.Second
}

func (ipPort *IPv4Port) SetIP(ip net.IP) {
	ipPort.First = ip
	ipPort.str = fmt.Sprintf("%s:%d", ipPort.First.String(), ipPort.Second)
}

func (ipPort *IPv4Port) SetPort(port int) {
	ipPort.Second = port
	ipPort.str = fmt.Sprintf("%s:%d", ipPort.First.String(), ipPort.Second)
}

func (ipPort IPv4Port) String() string {
	return ipPort.str
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

	ipPort.str = fmt.Sprintf("%s:%d", ipPort.First.String(), ipPort.Second)
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

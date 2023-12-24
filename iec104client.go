package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/riclolsen/go-iecp5/asdu"
	"github.com/riclolsen/go-iecp5/clog"
	"github.com/riclolsen/go-iecp5/cs104"
)

const (
	MAX_RTUS                  = 100
	I104M_DEFAULT_PORT        = "8099"
	I104M_DEFAULT_PORT_LISTEN = "8098"
	I104M_MSG_SIG             = 0x53535353
	I104M_MSG_SQ_SIG          = 0x64646464
)

type myClient struct{}
type rtuConfig struct {
	PrimaryAddress int
	Enabled        bool
	AllowCommands  bool
	Name           string
	IpPort         string
	IpPortBackup   string
	CommonAddress  int
	GiPeriod       int
	HasGiTicker    bool
}

type iec104Msg struct {
	ClientNumber int
	Asdu         *asdu.ASDU
}

type i104mMsg struct {
	Signature         uint32
	AddressOrQuantity uint32
	Type              uint32
	Primary           uint32
	Secondary         uint32
	Cause             uint32
	InfoSize          uint32
}

var sendOshmiChan chan iec104Msg // channel to send UDP messages to OSHMI

func readConfig(fname string) ([]rtuConfig, string, string) { // read config file for iec104 client
	iniData, err := readINIFile(fname)
	if err != nil {
		panic(err)
	}

	udpPort, ok := iniData["I104M"]["PORT"]
	if !ok {
		udpPort = I104M_DEFAULT_PORT
	}
	log.Println("I104M: will send messages to OSHMI on UDP port", udpPort)
	udpPortListen, ok := iniData["I104M"]["PORT_LISTEN"]
	if !ok {
		udpPortListen = I104M_DEFAULT_PORT_LISTEN
	}
	log.Println("I104M: will listen command messages from OSHMI on UDP port", udpPortListen)

	var configs []rtuConfig
	for i := 0; i < MAX_RTUS; i++ {
		ip, ok := iniData["RTU"+strconv.Itoa(i+1)]["IP_ADDRESS"]
		if !ok {
			break
		}
		ip_backup, ok := iniData["RTU"+strconv.Itoa(i+1)]["IP_ADDRESS_BACKUP"]
		if !ok {
			ip_backup = ip
		}
		port, ok := iniData["RTU"+strconv.Itoa(i+1)]["TCP_PORT"]
		if !ok {
			port = "2404"
		}
		enabled, ok := iniData["RTU"+strconv.Itoa(i+1)]["ENABLED"]
		if !ok {
			enabled = "1"
		}
		allow_cmd, ok := iniData["RTU"+strconv.Itoa(i+1)]["ALLOW_COMMANDS"]
		if !ok {
			allow_cmd = "1"
		}
		prim_addr, ok := iniData["RTU"+strconv.Itoa(i+1)]["PRIMARY_ADDRESS"]
		if !ok {
			prim_addr = "1"
		}
		prim_address, err := strconv.Atoi(prim_addr)
		if err != nil || prim_address <= 0 || prim_address > 255 {
			panic("invalid primary address")
		}
		name, ok := iniData["RTU"+strconv.Itoa(i+1)]["NAME"]
		if !ok {
			name = "RTU" + strconv.Itoa(i+1)
		}
		ca, ok := iniData["RTU"+strconv.Itoa(i+1)]["SECONDARY_ADDRESS"]
		if !ok {
			break
		}
		cai, err := strconv.Atoi(ca)
		if err != nil || cai <= 0 || cai > 255 {
			panic("invalid rtu address")
		}
		interval, ok := iniData["RTU"+strconv.Itoa(i+1)]["GI_PERIOD"]
		if !ok {
			interval = "330"
		}
		interval_gi, err := strconv.Atoi(interval)
		if err != nil || interval_gi < 0 {
			panic("invalid gi interval")
		}
		configs = append(configs, rtuConfig{
			Enabled:        strings.TrimSpace(enabled) != "0",
			AllowCommands:  strings.TrimSpace(allow_cmd) != "0",
			PrimaryAddress: prim_address,
			Name:           name,
			IpPort:         ip + ":" + port,
			IpPortBackup:   ip_backup + ":" + port,
			CommonAddress:  cai,
			GiPeriod:       interval_gi,
			HasGiTicker:    false,
		})
	}
	return configs, udpPort, udpPortListen
}

func handleI104mCmd(udpPortListen string) { // listen for I104M messages from OSHMI
}

func handleI104m(udpPort string) { // Forwards I104M messages to OSHMI
	const (
		structSize = int(unsafe.Sizeof(i104mMsg{}))
	)
	var ok bool
	var msg iec104Msg

	ipPort := "127.0.0.1:" + udpPort
	udpAddr, err := net.ResolveUDPAddr("udp", ipPort)
	if err != nil {
		panic("invalid upd address")
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		panic("can not open udp connection")
	}
	defer conn.Close()

	for {
		/*
			select {
			case msg, ok = <-sendOshmiChan: // wait for messages to forward
			case <-time.After(time.Second):
				// call timed out
				ok = false
			}
		*/
		msg, ok = <-sendOshmiChan
		var iMsg i104mMsg

		if ok {
			log.Println(">>>>>>>>>>>>>>>")
			log.Println(msg.Asdu)
			// conn.Write()
			log.Println(msg.Asdu.Identifier.Type)

			infoSize, _ := asdu.GetInfoObjSize(msg.Asdu.Type)
			asduData, err := msg.Asdu.MarshalBinary()
			data := asduData[6:]

			if err == nil {

				if msg.Asdu.Identifier.Variable.Number > 1 {
					iMsg = i104mMsg{
						Signature:         I104M_MSG_SQ_SIG,
						AddressOrQuantity: uint32(msg.Asdu.Variable.Number),
						Type:              uint32(msg.Asdu.Type),
						Primary:           uint32(msg.Asdu.OrigAddress),
						Secondary:         uint32(msg.Asdu.CommonAddr),
						Cause:             uint32(msg.Asdu.Coa.Cause),
						InfoSize:          uint32(infoSize),
					}
				} else {
					iMsg = i104mMsg{
						Signature:         I104M_MSG_SIG,
						AddressOrQuantity: uint32(data[2])*256*256 + uint32(data[1])*256 + uint32(data[0]),
						Type:              uint32(msg.Asdu.Type),
						Primary:           uint32(msg.Asdu.OrigAddress),
						Secondary:         uint32(msg.Asdu.CommonAddr),
						Cause:             uint32(msg.Asdu.Coa.Cause),
						InfoSize:          uint32(infoSize),
					}
				}

				dt := make([]byte, 0, structSize+len(data))
				buffer := bytes.NewBuffer(dt)
				buffer.Reset()
				binary.Write(buffer, binary.LittleEndian, iMsg.Signature)
				binary.Write(buffer, binary.LittleEndian, iMsg.AddressOrQuantity)
				binary.Write(buffer, binary.LittleEndian, iMsg.Type)
				binary.Write(buffer, binary.LittleEndian, iMsg.Primary)
				binary.Write(buffer, binary.LittleEndian, iMsg.Secondary)
				binary.Write(buffer, binary.LittleEndian, iMsg.Cause)
				binary.Write(buffer, binary.LittleEndian, iMsg.InfoSize)

				var udpBuf []byte
				if msg.Asdu.Identifier.Variable.Number == 1 { // just one value
					udpBuf = append(buffer.Bytes(), data[3:]...)
				} else if msg.Asdu.Identifier.Variable.Number > 1 && !msg.Asdu.Identifier.Variable.IsSequence {
					// many points [address, value], [address, value],...
					udpBuf = append(buffer.Bytes(), data...)
				} else { // address [value] [value],...
					udpBuf = buffer.Bytes()[:]
					addr := iMsg.AddressOrQuantity
					for i := 0; i < int(msg.Asdu.Identifier.Variable.Number); i++ {
						if len(data) >= 3+int(msg.Asdu.Identifier.Variable.Number)*infoSize {
							buffer.Reset()
							binary.Write(buffer, binary.LittleEndian, addr)
							udpBuf = append(udpBuf, buffer.Bytes()...)
							udpBuf = append(udpBuf, data[3+i*infoSize:3+(i+1)*infoSize]...)
							addr++
						}
					}
				}

				//log.Println(iMsg.Type)
				//log.Println(infoSize)
				//log.Println(udpBuf)
				conn.Write(udpBuf)
			}
		}
	}
}

func main() {

	configFile := "c:\\oshmi\\conf\\qtester104.ini"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	configs, udpPort, udpPortListen := readConfig(configFile)

	sendOshmiChan = make(chan iec104Msg, 10000) // channel

	for i := 0; i < len(configs); i++ {
		num := i
		if !configs[num].Enabled { // ignore disabled channel
			continue
		}
		time.Sleep(500 * time.Millisecond) // wait to start one connection at each .5s

		option := cs104.NewOption()
		if err := option.AddRemoteServer(configs[num].IpPort); err != nil {
			panic(err)
		}
		client := cs104.NewClient(&myClient{}, option)
		client.SetClientNumber(i)

		client.Clog = clog.NewLogger("client104 " + strconv.Itoa(num+1) + " => ")
		client.LogMode(true)

		client.SetOnConnectHandler(func(c *cs104.Client) {
			c.SendStartDt()
			go func() { // wait 3s then do a general interrogation
				time.Sleep(3 * time.Second)
				client.InterrogationCmd(asdu.CauseOfTransmission{
					IsTest:     false,
					IsNegative: false,
					Cause:      asdu.Activation,
				}, asdu.CommonAddr(configs[num].CommonAddress), asdu.QOIStation)
			}()

			// repeat GI interval
			if configs[num].HasGiTicker {
				return
			}

			go func() {
				configs[num].HasGiTicker = false
				for range time.NewTicker(time.Second * time.Duration(configs[num].GiPeriod)).C {
					if !client.IsConnected() {
						configs[num].HasGiTicker = false
						break
					}
					client.Debug("client104 "+strconv.Itoa(num+1)+" => ", "Request General Interrogation")
					client.InterrogationCmd(asdu.CauseOfTransmission{
						IsTest:     false,
						IsNegative: false,
						Cause:      asdu.Activation,
					}, asdu.CommonAddr(configs[num].CommonAddress), asdu.QOIStation)
				}
			}()
		})
		client.SetConnectionLostHandler(func(c *cs104.Client) {
			client.Debug("client104 "+strconv.Itoa(num+1)+" => ", "Connection lost")
		})
		err := client.Start()
		if err != nil {
			client.Warn("client104 "+strconv.Itoa(num+1)+" => failed to connect. error:%v", err)
		}
	}

	go handleI104m(udpPort) // handles udp message forwarding

	go handleI104mCmd(udpPortListen) // handles udp message forwarding

	for {
		time.Sleep(1 * time.Second)
	}
}

// fulfill cs104 client interface
func (myClient) InterrogationHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) CounterInterrogationHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) ReadHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) TestCommandHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) ClockSyncHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) ResetProcessHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) DelayAcquisitionHandler(asdu.Connect, *asdu.ASDU) error {
	return nil
}
func (myClient) ASDUHandler(c asdu.Connect, a *asdu.ASDU, s *cs104.Server, n int) error {
	log.Println("client104 " + strconv.Itoa(n+1) + " => Received asdu " + a.String())

	var msg = iec104Msg{
		ClientNumber: n,
		Asdu:         a,
	}
	sendOshmiChan <- msg

	return nil
}

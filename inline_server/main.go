package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync_server/common"
	"sync_server/global"
	"sync_server/network"
	"sync_server/pkg"
	"sync_server/util"
	"syscall"

	"github.com/go-redis/redis"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

var (
	env         string
	app         string
	signals     = make(chan os.Signal)
	host        string
	hostAddress string

	commandPort    int // grpc 命令下行服务端口
	serverType     string
	connectionType string // 连接类型 是否断开连接
	nativeServer   *network.NaiveServer
	redisClient    *redis.Client
	logLevel       string
	platForm       string
	protocol       string // protocol

	redisHost        string
	redisPort        int
	redisPassword    string
	redisDB          int
	redisReadTimeout int

	port            int
	socketTimeout   int
	sendCommandPort int
	messageChan     chan string
)

func init() {

	env = os.Getenv("ENV")
	app = os.Getenv("APP")
	if env == "" {
		env = "local"
	}
	if app == "" {
		app = "gateway"
	}

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	host = util.GetHost()
	if host == "" {
		log.Error().Msg("failed to get host name...")
	}
	messageChan = make(chan string, 100)
}

func readConfigFromFile() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	redisHost = viper.GetString("redis.host")
	redisPort = viper.GetInt("redis.port")
	redisPassword = viper.GetString("redis.password")
	redisDB = viper.GetInt("redis.db")

	commandPort = viper.GetInt("commandPort") // grpc
	serverType = viper.GetString("serverType")
	connectionType = viper.GetString("connectionType")
	socketTimeout = viper.GetInt("socketTimeout")
	port = viper.GetInt("port")

	logLevel = viper.GetString("logLevel")
	platForm = viper.GetString("platForm")
	switch logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	}
}

func main() {
	startServer()
}

func startServer() {
	hostAddress = util.GetLocalIP()
	readConfigFromFile()

	global.CommandPort = commandPort
	global.TTL = socketTimeout
	global.Protocol = protocol

	makeServer()
}

func HandMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("start to quit handle")
			return
		case msg := <-messageChan:
			SendGroupMessage(msg)
		}
	}
}

func SendGroupMessage(msg string) {
	connectionMap := nativeServer.GetConnectionMap()
	for deviceId, conn := range connectionMap {
		//log.Info().Msgf("device id: %s, connection type is: %s", deviceId, conn.DeviceType)
		if conn.DeviceType == common.Publisher {
			continue
		}
		conn.Send([]byte(msg + "\n"))
		log.Info().Msgf("sync to device: %s, data is: %s", deviceId, msg)
	}
}

func makeServer() {

	ctx := context.Background()
	//redisClient = util.GetRedisClientWithTimeOut(redisHost, redisPort, redisPassword, redisDB, redisReadTimeout)

	serverConfig := network.ServerConfig{
		Address: fmt.Sprintf("0.0.0.0:%d", port),
		Timeout: socketTimeout,
	}
	nativeServer = network.NewNativeServer(&serverConfig)
	nativeServer.RegisterCallbacks(connectionMade, connectionLost, messageReceived)
	done := make(chan bool, 1)

	go func() {
		sig := <-signals
		log.Info().Msgf("signal: %v", sig)
		nativeServer.Stop()
		done <- true
	}()

	go HandMessage(ctx)
	//if connectionType == common.ContinueConnection {
	//	go nativeServer.ContinueConnectionStore(redisClient, ctx) // 定时更新状态信息
	//}

	nativeServer.Listen()
	<-done
}

func connectionMade(c *network.Connection, deviceId string) {
	c.MarkConnection(deviceId)
	log.Info().Msgf("Receive new connection from device id: %s, device type is: %s", deviceId, c.DeviceType)

	//dataSet := make(map[string]interface{})
	//dataSet["deviceId"] = deviceId
	//dataSet["host"] = host
	//dataSet["address"] = hostAddress
	//dataSet["last_updated"] = time.Now().Unix()
	//vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, deviceId)
	//
	//.HMSet(vinKey, dataSet)
}

func messageReceived(c *network.Connection, data []byte) {

	segment := string(data)
	log.Info().Msgf("Receive segment: %s", segment)
	if len(c.Left) > 0 {
		segment = c.Left + segment
	}

	messages, _, _ := Split(segment)
	//log.Info().Msgf("message list is: %v", messages)
	for _, message := range messages {
		if c.IsFirstMessage {
			identityList := strings.Split(message, "@")
			deviceId := identityList[0]
			deviceType := identityList[1]

			if deviceType != "" {
				c.DeviceType = deviceType
			}
			connectionMade(c, deviceId)
			c.IsFirstMessage = false
		}
		messageChan <- message
	}
}

func connectionLost(c *network.Connection, err error) {
	deviceId := c.GetID()
	log.Info().Msgf("Connection lost with client, deviceId: %s, err: %v", deviceId, err)
	//dataSet := make(map[string]interface{})
	//dataSet["vin"] = deviceId
	//dataSet["status"] = common.OffLineStatus
	//dataSet["last_updated"] = time.Now().Unix()
	//vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, deviceId)
	//redisClient.HMSet(vinKey, dataSet)
}

// #device_server@publisher@32@up@end_string#g#
// #device_server@publisher@88@up@end_string#ring#
// #device_server@publisher@88@down@end_strin
func Split(segment string) (messages []string, left string, invalidMessage [][]byte) {

	if len(segment) < 12 {
		left = segment
		return
	}
	startFlag := "#"
	var indexList []int
	for index := 0; index < len(segment); index++ {
		sf := string(segment[index])
		if sf == startFlag {
			indexList = append(indexList, index)
		}
	}
	if len(indexList) == 1 {
		left = segment
	}
	var i int
	log.Info().Msgf("segment is: %s, index list is: %v", segment, indexList)
	for i < len(indexList) {
		message := segment[indexList[i] : indexList[i+1]+1]
		if len(message) == 0 {
			break
		}
		if message == "##" {
			break
		}
		log.Info().Msgf("message is: %s", message)
		ok := pkg.CheckPackage(message)

		if !ok {
			log.Error().Msgf("package error: %s", message)
			continue
		}
		messages = append(messages, message)
		i += 2
	}

	return
}

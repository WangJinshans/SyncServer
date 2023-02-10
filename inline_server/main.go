package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"sync_server/common"
	"sync_server/global"
	"sync_server/network"
	"sync_server/pkg"
	"sync_server/util"
	"syscall"
	"time"
)

var (
	old7e = []byte{0x7e}
	new7e = []byte{0x7d, 0x02}
	old7d = []byte{0x7d}
	new7d = []byte{0x7d, 0x01}
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
	maxQps          int64
	maxConnection   int
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

	log.Info().Msgf("serverType: %s", serverType)
	makeServer()
}

func makeServer() {

	ctx := context.Background()
	redisClient = util.GetRedisClientWithTimeOut(redisHost, redisPort, redisPassword, redisDB, redisReadTimeout)

	serverConfig := network.ServerConfig{
		Address:       fmt.Sprintf("0.0.0.0:%d", port),
		Timeout:       socketTimeout,
		MaxQps:        maxQps,
		MaxConnection: maxConnection,
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
	if connectionType == common.ContinueConnection {
		go nativeServer.ContinueConnectionStore(redisClient, ctx) // 定时更新状态信息
	}

	nativeServer.Listen()
	<-done
}

func connectionMade(c *network.Connection, deviceId string) {
	log.Info().Msgf("Receive new connection from device id: %s", deviceId)
	c.MarkConnection(deviceId)

	dataSet := make(map[string]interface{})
	dataSet["deviceId"] = deviceId
	dataSet["host"] = host
	dataSet["address"] = hostAddress
	dataSet["last_updated"] = time.Now().Unix()
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, deviceId)
	redisClient.HMSet(vinKey, dataSet)
}

func messageReceived(c *network.Connection, data []byte) {

	segment := string(data)
	log.Info().Msgf("Receive segment: %s", segment)
	if len(c.Left) > 0 {
		segment = c.Left + segment
	}

	messages, _, _ := Split(segment)
	log.Info().Msgf("message list is: %v", messages)
}

func connectionLost(c *network.Connection, err error) {
	deviceId := c.GetID()
	log.Info().Msgf("Connection lost with client, deviceId: %s, err: %v", deviceId, err)
	dataSet := make(map[string]interface{})
	dataSet["vin"] = deviceId
	dataSet["status"] = common.OffLineStatus
	dataSet["last_updated"] = time.Now().Unix()
	vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, deviceId)
	redisClient.HMSet(vinKey, dataSet)
}

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
	var i int
	for i < len(indexList) {
		message := segment[indexList[i] : indexList[i+1]+1]
		if len(message) == 0 {
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

// #device_id@type@key@operation@bcc#

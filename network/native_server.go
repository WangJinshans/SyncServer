package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync_server/common"
	"sync_server/global"
	"sync_server/util"
	"time"

	"github.com/go-redis/redis"

	"github.com/rs/zerolog/log"
)

var (
	mapMutex = sync.RWMutex{}
	hostName = ""
)

type Connection struct {
	conn           *net.TCPConn
	Server         Server
	Left           string
	id             string
	DeviceType     string // 发送或者接受
	MessageChan    chan []byte
	ExitChan       chan struct{}
	IsFirstMessage bool   // 是否是第一次发送消息
	Status         string // 连接状态  online: 活跃 offline: 离线
}

// ServerConfig involve server's configurations
type ServerConfig struct {
	Address       string
	Timeout       int
	MaxQps        int64
	MaxConnection int
}

type WarpedConnectionInfo struct {
	Data map[string]interface{}
	Conn *Connection
}

// NaiveServer represent a server
type NaiveServer struct {
	Config *ServerConfig

	funcOnConnectionMade   func(c *Connection, vin string)
	funcOnConnectionClosed func(c *Connection, err error)
	funcOnMessageReceived  func(c *Connection, message []byte)

	started           bool
	ConnectionChan    chan *Connection // 缓存VIN -- 登录时间 -- TTL
	RequestCount      int64            // 访问数
	QpsChan           chan struct{}    // qps数据通道
	QpsStartTimeStamp int64            // 开始采集qps指标的时间
	MaxQps            int64            // 配置最大qps,超出报警

	MaxConnection int // 最大连接数 超过拒绝连接

	idMap map[string]*Connection
}

func NewNativeServer(config *ServerConfig) (server *NaiveServer) {
	server = &NaiveServer{
		Config:         config,
		idMap:          make(map[string]*Connection),
		ConnectionChan: make(chan *Connection, 2000),
		QpsChan:        make(chan struct{}, 1000),
		MaxQps:         config.MaxQps,
		MaxConnection:  config.MaxConnection,
	}
	return
}

func init() {
	hostName = util.GetHost()
}

// SetID set id for connection
func (c *Connection) SetID(id string) {
	c.id = id
	c.Server.addConn(id, c)
}

func (c *Connection) MarkConnection(id string) (oldFlag bool) {
	old := c.Server.GetConn(id)
	// 保持长链接常常会出现游离的连接, 当新建一个连接时 如果存在老的连接 老的连接会被断开
	// 如果不存在老的连接 直接标记新连接
	if old != nil {
		// 存在未关闭的连接
		old.ExitChan <- struct{}{}
		c.Server.OnConnectionClosed(c, errors.New("connection refresh"))
		c.Server.removeConn(old.GetID())

		log.Info().Msgf("close old connection, vin: %s", id)
		oldFlag = true
	}
	c.SetID(id) // 设置当前连接Id
	c.Status = common.OnLineStatus
	return
}

func (c *Connection) GetID() string {
	return c.id
}

func (c *Connection) listen() {
	reader := bufio.NewReader(c.conn)

	var read int
	var err error
	// var buffers bytes.Buffer

	for {
		buffer := make([]byte, 4096, 4096)

		read, err = reader.Read(buffer)
		if err != nil {
			log.Error().Msgf("close connection: %s, error is: %v", c.id, err)
			c.ExitChan <- struct{}{}
			c.Server.OnConnectionClosed(c, err)
			c.Server.removeConn(c.id)
			return
		}
		if read > 0 {
			bs := make([]byte, read)
			// log.Info().Msgf("read is: %d, buffer[:read] is: %s", read, buffer[:read])
			// count := copy(bs, buffer[:read])
			copy(bs, buffer[:read])
			// log.Info().Msgf("copy count is: %d, bs is: %s", count, bs)
			c.MessageChan <- bs

		}

		// buffers.Write(buffer[:read])
		// c.MessageChan <- buffers.Bytes()
		// buffers.Reset()
		buffer = nil
	}
}

func (c *Connection) dispatchMessage() {
	for {
		select {
		case <-c.ExitChan:
			c.conn.Close()
			c.conn = nil
			log.Info().Msgf("stop working, connection:%s", c.id)
			return
		case message := <-c.MessageChan:
			c.Server.OnMessageReceived(c, message)
		}
	}
}

// Send send massage to peer
func (c *Connection) Send(message []byte) error {
	_, err := c.conn.Write(message)
	return err
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (s *NaiveServer) RegisterCallbacks(onConnectionMade func(c *Connection, vin string), onConnectionClosed func(c *Connection, err error), onMessageReceived func(c *Connection, message []byte)) {
	s.funcOnConnectionMade = onConnectionMade
	s.funcOnConnectionClosed = onConnectionClosed
	s.funcOnMessageReceived = onMessageReceived
}

func (s *NaiveServer) OnConnectionClosed(c *Connection, err error) {
	s.funcOnConnectionClosed(c, err)
}

func (s *NaiveServer) OnConnectionMade(c *Connection, vin string) {
	s.funcOnConnectionMade(c, vin)
}

func (s *NaiveServer) OnMessageReceived(c *Connection, message []byte) {
	s.funcOnMessageReceived(c, message)
}

// GetTimeout get timeout
func (s *NaiveServer) GetTimeout() time.Duration {
	return time.Duration(s.Config.Timeout) * time.Second
}

// addConn add a id-conn pair to connections
// called on package arrived
func (s *NaiveServer) addConn(id string, conn *Connection) {
	if id == "" {
		return
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	s.idMap[id] = conn
}

// removeConn remove a named connection to connections
// usually called on connection lost
func (s *NaiveServer) removeConn(id string) {
	if id == "" {
		return
	}

	mapMutex.Lock()
	defer mapMutex.Unlock()

	delete(s.idMap, id)
}

func (s *NaiveServer) GetConn(id string) *Connection {
	mapMutex.RLock()
	defer mapMutex.RUnlock()

	return s.idMap[id]
}

func (s *NaiveServer) GetConnectionMap() map[string]*Connection {
	return s.idMap
}
func (s *NaiveServer) Listen() {
	s.started = true
	defer func() { s.started = false }()

	addr, err := net.ResolveTCPAddr("tcp", s.Config.Address)
	if err != nil {
		log.Panic().Err(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panic().Err(err)
	}
	defer listener.Close()

	for {
		if !s.started {
			log.Info().Msg("server is going down")
			return
		}
		err = listener.SetDeadline(time.Now().Add(3 * time.Second))
		if err != nil {
			log.Error().Msg(err.Error())
			return
		}
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if err != nil {
			continue
		}

		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(300 * time.Second)

		c := Connection{
			conn:           conn,
			Server:         s,
			MessageChan:    make(chan []byte, 100),
			DeviceType:     common.Receiver,
			ExitChan:       make(chan struct{}),
			IsFirstMessage: true,
		}
		log.Info().Msgf("receive new connection: %s", conn.RemoteAddr())

		go c.dispatchMessage()
		go c.listen()
	}
}

// 空结构体不占内存
func (s *NaiveServer) SubmitRequest() {
	s.QpsChan <- struct{}{}
}

func (s *NaiveServer) CalculateQps(ctx context.Context) {
	log.Info().Msg("start to calculate qps...")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("qps server is about to quit...")
			return
		case <-s.QpsChan:
			s.RequestCount += 1
			if s.QpsStartTimeStamp != 0 {
				s.QpsStartTimeStamp = time.Now().Unix()
			} else {
				timeStamp := time.Now().Unix()
				gap := timeStamp - s.QpsStartTimeStamp
				if gap > 1000 {
					qps := s.RequestCount
					if s.MaxQps > 0 && (qps > s.MaxQps) {
						log.Info().Msgf("warning, qps now: %d exceeded the max qps: %d", qps, s.MaxQps)
						s.QpsStartTimeStamp = 0
						s.RequestCount = 0 // 清空
					}
				}
			}
		}
	}
}

func (s *NaiveServer) Stop() {
	s.started = false
}

// 更新时间以及ttl由外部传递
func (s *NaiveServer) ConnectionStore(redisClient *redis.Client, ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	vinSet := make(map[string]bool, 5000)
	var dataList []WarpedConnectionInfo
	for {
		select {
		case <-ctx.Done():
			return
		case conn := <-s.ConnectionChan:
			var info WarpedConnectionInfo
			data := make(map[string]interface{})
			vin := conn.GetID()
			_, ok := vinSet[vin]
			if !ok {
				vinSet[vin] = true
				data["vin"] = vin
				data["host"] = hostName
				data["last_updated"] = time.Now().Unix()
				info.Data = data
				info.Conn = conn
				dataList = append(dataList, info)
			}
		case <-timer.C:
			if len(dataList) > 0 {
				data := make([]WarpedConnectionInfo, len(dataList))
				copy(data, dataList)
				dataList = nil
				vinSet = make(map[string]bool, 5000)
				updateConnections(redisClient, data, common.RegularConnection)
			}
		}
	}
}

// TODO 确认TTL过期时间以及数据更新时间 避免造成缺口
// FIXME 理论上来说存在被删除之后再次写入的可能,毫秒级别的误差,写入之后只能自然过期
// 添加connection_host 存储host信息
func updateConnections(redisClient *redis.Client, dataList []WarpedConnectionInfo, connectionType string) {
	pipe := redisClient.Pipeline()
	for _, info := range dataList {
		conn := info.Conn
		item := info.Data
		vin := item["vin"]

		vinKey := fmt.Sprintf("%s_%s", common.ConnectionKey, vin)

		if conn.conn != nil { // 防止离线删除后又因更新写入redis,而实则已经离线直到过期后才会被删除
			pipe.HMSet(vinKey, item)
			// 不断开类型的连接不设置过期时间
			if connectionType == common.RegularConnection {
				pipe.ExpireAt(vinKey, time.Now().Add(time.Duration(global.TTL)*time.Second))
			}
		}
	}
	if _, err := pipe.Exec(); err != nil {
		log.Error().Msgf("failed to push connections to redis: %s", err.Error())
		return
	}
	return
}

// 长连接版本
// 离线上报的时候 更新连接状态 收到实时数据更新连接状态
func (s *NaiveServer) ContinueConnectionStore(redisClient *redis.Client, ctx context.Context) {
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()
	vinSet := make(map[string]bool, 5000)
	var dataList []WarpedConnectionInfo
	var deferList []WarpedConnectionInfo

	for {
		select {
		case <-ctx.Done():
			return
		case conn := <-s.ConnectionChan:
			var info WarpedConnectionInfo
			data := make(map[string]interface{})
			vin := conn.GetID()
			_, ok := vinSet[vin]
			if ok {
				// 存在连接更新信息 放到延迟队列 确保更新成最后状态
				if conn.Status == common.OffLineStatus {
					data["vin"] = vin
					data["host"] = hostName
					data["status"] = common.OffLineStatus // 连接状态
					data["last_updated"] = time.Now().Unix()
					info.Data = data
					info.Conn = conn
					deferList = append(deferList, info)
				} else {
					data["vin"] = vin
					data["host"] = hostName
					data["status"] = common.OnLineStatus // 连接状态
					data["last_updated"] = time.Now().Unix()
					info.Data = data
					info.Conn = conn
					deferList = append(deferList, info)
				}
			} else {
				vinSet[vin] = true
				data["vin"] = vin
				data["host"] = hostName
				data["status"] = conn.Status // 连接状态
				data["last_updated"] = time.Now().Unix()
				info.Data = data
				info.Conn = conn
				dataList = append(dataList, info)
			}
		case <-timer.C:
			if len(dataList) > 0 {
				data := make([]WarpedConnectionInfo, len(dataList))
				copy(data, dataList)
				dataList = nil
				vinSet = make(map[string]bool, 5000)
				updateConnections(redisClient, data, common.ContinueConnection)
			}
			if len(deferList) > 0 {
				data := make([]WarpedConnectionInfo, len(deferList))
				copy(data, deferList)
				deferList = nil
				updateConnections(redisClient, data, common.ContinueConnection)
			}
		}
	}
}

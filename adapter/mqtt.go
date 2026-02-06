package adapter

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gonglijing/xunjiFsu/internal/models"
)

// MQTTAdapter MQTT适配器
// 配置 JSON:
// {"broker":"tcp://127.0.0.1:1883","topic":"gogw/data","alarm_topic":"gogw/alarm"}
type MQTTAdapter struct {
	config      string
	broker      string
	topic       string
	alarmTopic  string
	clientID    string
	username    string
	password    string
	qos         byte
	retain      bool
	timeout     time.Duration
	keepAlive   time.Duration
	lastUpload  time.Time
	client      mqtt.Client
	mu          sync.RWMutex
	initialized bool
}

// MQTTConfig MQTT配置
// KeepAlive/ConnectTimeout 单位秒
// QOS 范围 0-2
// Retain 是否保留
//
// Example:
// {"broker":"tcp://127.0.0.1:1883","topic":"gogw/data","alarm_topic":"gogw/alarm"}
type MQTTConfig struct {
	Broker         string `json:"broker"`
	Topic          string `json:"topic"`
	AlarmTopic     string `json:"alarm_topic"`
	ClientID       string `json:"client_id"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	QOS            int    `json:"qos"`
	Retain         bool   `json:"retain"`
	KeepAlive      int    `json:"keep_alive"`
	ConnectTimeout int    `json:"connect_timeout"`
}

// NewMQTTAdapter 创建MQTT适配器
func NewMQTTAdapter() *MQTTAdapter {
	return &MQTTAdapter{
		lastUpload: time.Time{},
	}
}

// Name 获取名称
func (a *MQTTAdapter) Name() string {
	return "mqtt"
}

// Initialize 初始化
func (a *MQTTAdapter) Initialize(configStr string) error {
	cfg := &MQTTConfig{}
	if err := json.Unmarshal([]byte(configStr), cfg); err != nil {
		return fmt.Errorf("failed to parse MQTT config: %w", err)
	}
	if cfg.Broker == "" {
		return fmt.Errorf("broker is required")
	}
	if cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}

	a.config = configStr
	a.broker = normalizeBroker(cfg.Broker)
	a.topic = cfg.Topic
	a.alarmTopic = cfg.AlarmTopic
	if a.alarmTopic == "" {
		a.alarmTopic = a.topic + "/alarm"
	}
	a.clientID = cfg.ClientID
	if a.clientID == "" {
		a.clientID = fmt.Sprintf("gogw-mqtt-%d", time.Now().UnixNano())
	}
	a.username = cfg.Username
	a.password = cfg.Password
	a.qos = clampQOS(cfg.QOS)
	a.retain = cfg.Retain
	if cfg.ConnectTimeout > 0 {
		a.timeout = time.Duration(cfg.ConnectTimeout) * time.Second
	} else {
		a.timeout = 10 * time.Second
	}
	if cfg.KeepAlive > 0 {
		a.keepAlive = time.Duration(cfg.KeepAlive) * time.Second
	}

	client, err := connectMQTT(a.broker, a.clientID, a.username, a.password, cfg.KeepAlive, cfg.ConnectTimeout)
	if err != nil {
		return err
	}
	a.client = client
	a.initialized = true
	return nil
}

// Send 发送数据
func (a *MQTTAdapter) Send(data *models.CollectData) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	msg := map[string]interface{}{
		"device_name": data.DeviceName,
		"timestamp":   data.Timestamp,
		"fields":      data.Fields,
	}
	body, _ := json.Marshal(msg)
	return a.publish(a.topic, body)
}

// SendAlarm 发送报警
func (a *MQTTAdapter) SendAlarm(alarm *models.AlarmPayload) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	body, _ := json.Marshal(alarm)
	return a.publish(a.alarmTopic, body)
}

// Close 关闭
func (a *MQTTAdapter) Close() error {
	a.initialized = false
	if a.client != nil && a.client.IsConnected() {
		a.client.Disconnect(250)
	}
	return nil
}

func (a *MQTTAdapter) publish(topic string, payload []byte) error {
	if topic == "" {
		return fmt.Errorf("topic is empty")
	}
	client, timeout, qos, retain, err := a.ensureClient()
	if err != nil {
		return err
	}
	token := client.Publish(topic, qos, retain, payload)
	if !token.WaitTimeout(timeout) {
		return fmt.Errorf("mqtt publish timeout")
	}
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

func (a *MQTTAdapter) ensureClient() (mqtt.Client, time.Duration, byte, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client == nil {
		return nil, 0, 0, false, fmt.Errorf("mqtt client not initialized")
	}
	if !a.client.IsConnected() {
		token := a.client.Connect()
		if !token.WaitTimeout(a.timeout) {
			return nil, 0, 0, false, fmt.Errorf("mqtt connect timeout")
		}
		if err := token.Error(); err != nil {
			return nil, 0, 0, false, err
		}
	}
	return a.client, a.timeout, a.qos, a.retain, nil
}

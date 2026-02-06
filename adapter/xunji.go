package adapter

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gonglijing/xunjiFsu/internal/models"
)

// XunJiAdapter 循迹适配器
// 配置 JSON 参考 models.XunJiConfig
// serverUrl 必填，topic/alarmTopic 可选
//
// Example:
// {"productKey":"pk","deviceKey":"dk","serverUrl":"tcp://127.0.0.1:1883"}
type XunJiAdapter struct {
	config      *models.XunJiConfig
	client      mqtt.Client
	topic       string
	alarmTopic  string
	qos         byte
	retain      bool
	timeout     time.Duration
	lastUpload  time.Time
	mu          sync.RWMutex
	initialized bool
}

// NewXunJiAdapter 创建循迹适配器
func NewXunJiAdapter() *XunJiAdapter {
	return &XunJiAdapter{
		lastUpload: time.Time{},
	}
}

// Name 获取名称
func (a *XunJiAdapter) Name() string {
	return "xunji"
}

// Initialize 初始化
func (a *XunJiAdapter) Initialize(configStr string) error {
	config := &models.XunJiConfig{}
	if err := json.Unmarshal([]byte(configStr), config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	if config.ServerURL == "" {
		return fmt.Errorf("serverUrl is required")
	}
	if config.ProductKey == "" || config.DeviceKey == "" {
		return fmt.Errorf("productKey and deviceKey are required")
	}
	a.config = config

	broker := normalizeBroker(config.ServerURL)
	clientID := config.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("xunji-%s-%s-%d", config.ProductKey, config.DeviceKey, time.Now().UnixNano())
	}
	a.qos = clampQOS(config.QOS)
	a.retain = config.Retain
	a.topic = config.Topic
	if a.topic == "" {
		a.topic = fmt.Sprintf("xunji/%s/%s", config.ProductKey, config.DeviceKey)
	}
	a.alarmTopic = config.AlarmTopic
	if a.alarmTopic == "" {
		a.alarmTopic = a.topic + "/alarm"
	}
	if config.Timeout > 0 {
		a.timeout = time.Duration(config.Timeout) * time.Second
	} else {
		a.timeout = 10 * time.Second
	}

	client, err := connectMQTT(broker, clientID, config.Username, config.Password, config.KeepAlive, config.Timeout)
	if err != nil {
		return err
	}
	a.client = client
	a.initialized = true
	return nil
}

// Send 发送数据
func (a *XunJiAdapter) Send(data *models.CollectData) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	message := a.buildMessage(data)
	if err := a.publish(a.topic, []byte(message)); err != nil {
		return err
	}
	return nil
}

// SendAlarm 发送报警
func (a *XunJiAdapter) SendAlarm(alarm *models.AlarmPayload) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	message := a.buildAlarmMessage(alarm)
	if err := a.publish(a.alarmTopic, []byte(message)); err != nil {
		return err
	}
	return nil
}

// buildMessage 构建循迹消息
func (a *XunJiAdapter) buildMessage(data *models.CollectData) string {
	properties := make(map[string]interface{})
	for key, value := range data.Fields {
		properties[key] = value
	}

	msg := map[string]interface{}{
		"id":      fmt.Sprintf("msg_%d", time.Now().UnixNano()),
		"version": "1.0",
		"sys": map[string]interface{}{
			"ack": 1,
		},
		"params": map[string]interface{}{
			"properties": properties,
			"events":     map[string]interface{}{},
			"subDevices": []interface{}{
				map[string]interface{}{
					"identity": map[string]string{
						"productKey": data.ProductKey,
						"deviceKey":  data.DeviceKey,
					},
					"properties": properties,
				},
			},
		},
	}

	jsonBytes, _ := json.Marshal(msg)
	return string(jsonBytes)
}

// buildAlarmMessage 构建报警消息
func (a *XunJiAdapter) buildAlarmMessage(alarm *models.AlarmPayload) string {
	eventValue := map[string]interface{}{
		"field_name":   alarm.FieldName,
		"actual_value": alarm.ActualValue,
		"threshold":    alarm.Threshold,
		"operator":     alarm.Operator,
		"message":      alarm.Message,
	}

	event := map[string]interface{}{
		"value": eventValue,
		"time":  time.Now().UnixMilli(),
	}

	events := map[string]interface{}{
		"alarm": event,
	}

	msg := map[string]interface{}{
		"id":      fmt.Sprintf("alarm_%d", time.Now().UnixNano()),
		"version": "1.0",
		"sys": map[string]interface{}{
			"ack": 1,
		},
		"params": map[string]interface{}{
			"properties": map[string]interface{}{},
			"events":     events,
			"subDevices": []interface{}{
				map[string]interface{}{
					"identity": map[string]string{
						"productKey": alarm.ProductKey,
						"deviceKey":  alarm.DeviceKey,
					},
					"properties": map[string]interface{}{},
					"events":     events,
				},
			},
		},
	}

	jsonBytes, _ := json.Marshal(msg)
	return string(jsonBytes)
}

// Close 关闭
func (a *XunJiAdapter) Close() error {
	a.initialized = false
	a.config = nil
	if a.client != nil && a.client.IsConnected() {
		a.client.Disconnect(250)
	}
	return nil
}

func (a *XunJiAdapter) publish(topic string, payload []byte) error {
	if topic == "" {
		return fmt.Errorf("topic is empty")
	}
	a.mu.Lock()
	client := a.client
	timeout := a.timeout
	qos := a.qos
	retain := a.retain
	a.mu.Unlock()
	if client == nil {
		return fmt.Errorf("mqtt client not initialized")
	}
	if !client.IsConnected() {
		token := client.Connect()
		if !token.WaitTimeout(timeout) {
			return fmt.Errorf("mqtt connect timeout")
		}
		if err := token.Error(); err != nil {
			return err
		}
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

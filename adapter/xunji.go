package adapter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	seq         uint64
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
	if !strings.HasPrefix(a.topic, "/sys/") {
		a.topic = fmt.Sprintf("/sys/%s/%s/thing/event/property/pack/post", config.ProductKey, config.DeviceKey)
	}
	a.alarmTopic = config.AlarmTopic
	if !strings.HasPrefix(a.alarmTopic, "/sys/") {
		a.alarmTopic = a.topic
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
	a.subscribeCommandTopics(client)
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
		properties[key] = convertFieldValue(value)
	}
	subPK := strings.TrimSpace(data.ProductKey)
	subDK := strings.TrimSpace(data.DeviceKey)
	if subPK == "" {
		subPK = a.config.ProductKey
	}
	if subDK == "" {
		subDK = a.config.DeviceKey
	}

	msg := map[string]interface{}{
		"id":      a.nextID("msg"),
		"version": "1.0",
		"sys": map[string]interface{}{
			"ack": 0,
		},
		"method": "thing.event.property.pack.post",
		"params": map[string]interface{}{
			"properties": map[string]interface{}{},
			"events":     map[string]interface{}{},
			"subDevices": []interface{}{
				map[string]interface{}{
					"identity": map[string]string{
						"productKey": subPK,
						"deviceKey":  subDK,
					},
					"properties": properties,
					"events":     map[string]interface{}{},
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
		"id":      a.nextID("alarm"),
		"version": "1.0",
		"sys": map[string]interface{}{
			"ack": 0,
		},
		"method": "thing.event.property.pack.post",
		"params": map[string]interface{}{
			"properties": map[string]interface{}{},
			"events":     map[string]interface{}{},
			"subDevices": []interface{}{
				map[string]interface{}{
					"identity": map[string]string{
						"productKey": pickFirstNonEmpty(alarm.ProductKey, a.config.ProductKey),
						"deviceKey":  pickFirstNonEmpty(alarm.DeviceKey, a.config.DeviceKey),
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
		a.subscribeCommandTopics(client)
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

func (a *XunJiAdapter) subscribeCommandTopics(client mqtt.Client) {
	if a.config == nil {
		return
	}
	pk := strings.TrimSpace(a.config.ProductKey)
	dk := strings.TrimSpace(a.config.DeviceKey)
	if pk == "" || dk == "" {
		return
	}

	a.subscribe(client, fmt.Sprintf("/sys/%s/%s/thing/service/property/set", pk, dk), a.handlePropertySet)
	a.subscribe(client, fmt.Sprintf("/sys/%s/%s/thing/service/+", pk, dk), a.handleServiceCall)
	a.subscribe(client, fmt.Sprintf("/sys/%s/%s/thing/config/push", pk, dk), a.handleConfigPush)
}

func (a *XunJiAdapter) subscribe(client mqtt.Client, topic string, handler mqtt.MessageHandler) {
	token := client.Subscribe(topic, a.qos, handler)
	if !token.WaitTimeout(a.timeout) {
		return
	}
}

func (a *XunJiAdapter) handlePropertySet(_ mqtt.Client, message mqtt.Message) {
	pk, dk, ok := extractIdentity(message.Topic())
	if !ok {
		return
	}
	var req struct {
		Id     string                 `json:"id"`
		Params map[string]interface{} `json:"params"`
	}
	if err := json.Unmarshal(message.Payload(), &req); err != nil {
		return
	}
	resp := map[string]interface{}{
		"code":    200,
		"data":    req.Params,
		"id":      req.Id,
		"message": "success",
		"version": "1.0.0",
	}
	respBody, _ := json.Marshal(resp)
	_ = a.publish(fmt.Sprintf("/sys/%s/%s/thing/service/property/set_reply", pk, dk), respBody)
}

func (a *XunJiAdapter) handleServiceCall(_ mqtt.Client, message mqtt.Message) {
	parts := splitTopic(message.Topic())
	if len(parts) != 7 {
		return
	}
	pk, dk, svc := parts[1], parts[2], parts[6]
	if strings.HasSuffix(svc, "reply") {
		return
	}
	var req struct {
		Id string `json:"id"`
	}
	if err := json.Unmarshal(message.Payload(), &req); err != nil {
		return
	}
	resp := map[string]interface{}{
		"code":    200,
		"data":    map[string]interface{}{},
		"id":      req.Id,
		"message": "success",
		"version": "1.0.0",
	}
	respBody, _ := json.Marshal(resp)
	_ = a.publish(fmt.Sprintf("/sys/%s/%s/thing/service/%s_reply", pk, dk, svc), respBody)
}

func (a *XunJiAdapter) handleConfigPush(_ mqtt.Client, message mqtt.Message) {
	pk, dk, ok := extractIdentity(message.Topic())
	if !ok {
		return
	}
	var req struct {
		Id string `json:"id"`
	}
	if err := json.Unmarshal(message.Payload(), &req); err != nil {
		return
	}
	resp := map[string]interface{}{
		"code": 200,
		"data": map[string]interface{}{},
		"id":   req.Id,
	}
	respBody, _ := json.Marshal(resp)
	_ = a.publish(fmt.Sprintf("/sys/%s/%s/thing/config/push/reply", pk, dk), respBody)
}

func convertFieldValue(value string) interface{} {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return value
}

func (a *XunJiAdapter) nextID(prefix string) string {
	n := atomic.AddUint64(&a.seq, 1)
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixMilli(), n)
}

func pickFirstNonEmpty(values ...string) string {
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v != "" {
			return v
		}
	}
	return ""
}

func splitTopic(topic string) []string {
	raw := strings.Split(strings.TrimSpace(topic), "/")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func extractIdentity(topic string) (string, string, bool) {
	parts := splitTopic(topic)
	if len(parts) < 4 || parts[0] != "sys" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

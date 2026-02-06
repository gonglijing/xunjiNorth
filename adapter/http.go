package adapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gonglijing/xunjiFsu/internal/models"
)

// HTTPAdapter HTTP适配器
// 配置 JSON:
// {"url":"http://...","headers":{"k":"v"},"timeout":30}
type HTTPAdapter struct {
	config      string
	url         string
	headers     map[string]string
	lastUpload  time.Time
	timeout     time.Duration
	mu          sync.RWMutex
	initialized bool
}

// HTTPConfig HTTP配置
// Timeout 单位秒
// Headers 为额外请求头
// URL 为目标地址
//
// Example:
// {"url":"http://127.0.0.1:8080/ingest","headers":{"Authorization":"Bearer x"},"timeout":30}
type HTTPConfig struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Timeout int               `json:"timeout"`
}

// NewHTTPAdapter 创建HTTP适配器
func NewHTTPAdapter() *HTTPAdapter {
	return &HTTPAdapter{
		lastUpload: time.Time{},
		timeout:    30 * time.Second,
	}
}

// Name 获取名称
func (a *HTTPAdapter) Name() string {
	return "http"
}

// Initialize 初始化
func (a *HTTPAdapter) Initialize(configStr string) error {
	config := &HTTPConfig{}
	if err := json.Unmarshal([]byte(configStr), config); err != nil {
		return fmt.Errorf("failed to parse HTTP config: %w", err)
	}

	if config.URL == "" {
		return fmt.Errorf("url is required")
	}

	a.config = configStr
	a.url = config.URL
	a.headers = config.Headers
	if config.Timeout > 0 {
		a.timeout = time.Duration(config.Timeout) * time.Second
	}
	a.initialized = true

	log.Printf("HTTP adapter initialized: %s", a.url)
	return nil
}

// Send 发送数据
func (a *HTTPAdapter) Send(data *models.CollectData) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	msg := map[string]interface{}{
		"device_name": data.DeviceName,
		"timestamp":   data.Timestamp,
		"fields":      data.Fields,
	}

	body, _ := json.Marshal(msg)
	return a.sendRequest(a.url, body, "data")
}

// SendAlarm 发送报警
func (a *HTTPAdapter) SendAlarm(alarm *models.AlarmPayload) error {
	if !a.initialized {
		return fmt.Errorf("adapter not initialized")
	}

	body, _ := json.Marshal(alarm)
	return a.sendRequest(a.url, body, "alarm")
}

// sendRequest 发送HTTP请求
func (a *HTTPAdapter) sendRequest(url string, body []byte, msgType string) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range a.headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: a.timeout}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("HTTP %s request failed: %v", msgType, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("HTTP %s sent successfully to %s", msgType, url)
	return nil
}

// Close 关闭
func (a *HTTPAdapter) Close() error {
	a.initialized = false
	return nil
}

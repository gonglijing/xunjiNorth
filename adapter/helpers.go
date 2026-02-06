package adapter

import (
	"fmt"
	"log"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func connectMQTT(broker, clientID, username, password string, keepAliveSec, timeoutSec int) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(clientID)
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	if keepAliveSec > 0 {
		opts.SetKeepAlive(time.Duration(keepAliveSec) * time.Second)
	}
	if timeoutSec > 0 {
		opts.SetConnectTimeout(time.Duration(timeoutSec) * time.Second)
	} else {
		opts.SetConnectTimeout(10 * time.Second)
	}
	opts.OnConnectionLost = func(_ mqtt.Client, err error) {
		if err != nil {
			log.Printf("MQTT connection lost: %v", err)
		}
	}
	opts.OnConnect = func(_ mqtt.Client) {
		log.Printf("MQTT connected: %s", broker)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	timeout := 10 * time.Second
	if timeoutSec > 0 {
		timeout = time.Duration(timeoutSec) * time.Second
	}
	if !token.WaitTimeout(timeout) {
		return nil, fmt.Errorf("mqtt connect timeout")
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return client, nil
}

func normalizeBroker(broker string) string {
	if broker == "" {
		return ""
	}
	if strings.Contains(broker, "://") {
		return broker
	}
	return "tcp://" + broker
}

func clampQOS(qos int) byte {
	if qos < 0 {
		return 0
	}
	if qos > 2 {
		return 2
	}
	return byte(qos)
}

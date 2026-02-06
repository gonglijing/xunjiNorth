package main

import (
	"github.com/gonglijing/xunjiFsu/internal/northbound"
	"github.com/gonglijing/xunjiFsu/plugin_north/adapter"
	plugin "github.com/hashicorp/go-plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: northbound.NorthboundHandshake,
		Plugins: map[string]plugin.Plugin{
			northbound.NorthboundPluginName: &northbound.NorthboundPlugin{Impl: adapter.NewHTTPAdapter()},
		},
	})
}

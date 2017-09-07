package beater

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/goomzee/cassandrabeat/config"
)

type Cassandrabeat struct {
	done       chan struct{}
	config     config.Config
	client     publisher.Client

	table      []string
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Cassandrabeat{
		done: make(chan struct{}),
		config: config,
	}
	return bt, nil
}

func (bt *Cassandrabeat) Run(b *beat.Beat) error {
	logp.Info("cassandrabeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	bt.table = bt.config.Table[:]
	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		for _, table := range bt.table {
			logp.Info("Getting latency for table: %s", table)
			bt.getLatency(table)
		}
		logp.Info("Event sent")
	}
}

func (bt *Cassandrabeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Cassandrabeat) getLatency(table string) {
	keyspace := strings.Split(table, ".")

	cmdName := "nodetool"
	cmdArgs := []string{"-h", bt.config.Host, "cfstats", "-F", "json", table}
	cmdOut := exec.Command(cmdName, cmdArgs...).Output

	output, _ := cmdOut()
	var cfstats map[string]interface{}
	json.Unmarshal(output, &cfstats)

	read_latency_ms := 0.0
	if cfstats[keyspace[0]].(map[string]interface{})["read_latency_ms"] != nil {
		read_latency_ms = cfstats[keyspace[0]].(map[string]interface{})["read_latency_ms"].(float64)
	}

	write_latency_ms := 0.0
	if cfstats[keyspace[0]].(map[string]interface{})["write_latency_ms"] != nil {
		write_latency_ms = cfstats[keyspace[0]].(map[string]interface{})["write_latency_ms"].(float64)
	}

	event := common.MapStr {
		"@timestamp":	             common.Time(time.Now()),
		"keyspace_name":             keyspace[0],
		"keyspace_read_count":       cfstats[keyspace[0]].(map[string]interface{})["read_count"].(float64),
		"keyspace_read_latency_ms":  read_latency_ms,
		"keyspace_write_count":      cfstats[keyspace[0]].(map[string]interface{})["write_count"].(float64),
		"keyspace_write_latency_ms": write_latency_ms,
		"table":                     cfstats[keyspace[0]].(map[string]interface{})["tables"].(map[string]interface{})[keyspace[1]].(map[string]interface{}),
		"table_name":	             table,
	}

	bt.client.PublishEvent(event)
}

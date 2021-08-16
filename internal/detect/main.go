package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
)

var ctx = context.Background()

const (
	redisPong = "PONG"
	redisCommandScan = "scan"
	redisRoleMaster = "master"
	defaultScanCursor = 0
)

func main() {
	scanNodes()
}

func scanNodes() {
	client := redis.NewClient(&redis.Options{
		Addr:     "host:port",
		Password: "xxx",
	})
	defer client.Close()

	pingRes, err := client.Ping(ctx).Result()
	if err != nil || !strings.EqualFold(pingRes, redisPong) {
		fmt.Println("new client err!", err)
		return
	}

	clusterNodesRes, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		fmt.Println("list nodes err!", err)
		return
	}

	clusterNodes := strings.Split(clusterNodesRes, "\n")
	for _, node := range clusterNodes {
		nodeInfos := strings.Split(node, " ")
		if len(nodeInfos) != 9 {
			// do nothing
			continue
		}
		nodeId := nodeInfos[0]
		role := nodeInfos[2]
		if !strings.Contains(role, redisRoleMaster) {
			continue
		}
		doScan(client, nodeId)
	}
}

func doScan(client *redis.Client, nodeId string) {
	scanNode := func(ctx context.Context, client *redis.Client, cursor uint64, nodeId string) *redis.Cmd {
		cmd := redis.NewCmd(ctx, redisCommandScan, cursor, nodeId)
		_ = client.Process(ctx, cmd)
		return cmd
	}
	scanNodeRes, err := scanNode(ctx, client, defaultScanCursor, nodeId).Result()
	if err != nil {
		fmt.Println("do scan err!", err)
	}
	readScanReply(nodeId, scanNodeRes)
}

func readScanReply(nodeId string, scanVal interface{}) {
	switch scanValType := scanVal.(type) {
	case []interface{}:
		scanRes, ok := scanVal.([]interface{})
		if ok {
			if len(scanRes) != 2 {
				fmt.Printf("got %d elements in scan reply, expected 2\n", len(scanRes))
			}
			//fmt.Println("cursor:", scanRes[0])
			//fmt.Println("page:",scanRes[1])

			cursor, err := strconv.ParseInt(scanRes[0].(string), 10, 64)
			if err != nil {
				fmt.Println("parse cursor err!", err)
				return
			}
			if cursor >= 0 {
				fmt.Printf("node[%s] is ok!\n", nodeId)
			}
		} else {
			fmt.Println("parse err!")
		}
	default:
		fmt.Printf("got %d type in scan reply, expected []interface{} \n", scanValType)
		return
	}
}

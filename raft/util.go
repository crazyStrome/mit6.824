package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// 心跳间隔
const heartbeatDuration = 150 * time.Millisecond

// 延时间隔
const sleepDuration = 10 * time.Millisecond

var elecrand = rand.New(rand.NewSource(time.Now().Unix()))

//生成随机选举延时
func electionTimeoutGenerator() time.Duration {
	var start = 1200
	var end = 1500
	return time.Duration(start+elecrand.Intn(end-start)) * time.Millisecond
}

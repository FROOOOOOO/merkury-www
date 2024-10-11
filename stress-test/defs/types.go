package defs

import (
	"context"
	"time"
)

type empty struct{}
type ActionMapper map[string]map[string][]string
type GroupMapper map[string][]string
type ResourceMapper map[string]map[string]empty
type ResourceStore map[string]empty
type Config struct {
	Debug         bool
	Conn          int
	Num           int
	Action        string
	Duration      time.Duration
	Anntation     int
	LabelSelector string
	Auth          string
}
type Meta struct {
	Res       string
	Namespace string
}
type Stress interface {
	Run(context.Context)
	Info() (string, string, string, int, int, time.Duration, []int, []int, []int)
}

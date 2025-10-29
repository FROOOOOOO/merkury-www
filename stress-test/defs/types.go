/*
Copyright 2025 FROOOOOOO and Ma-YuXin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

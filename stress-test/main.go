package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"stressTest/defs"
	"stressTest/pkg/delete"
	"stressTest/pkg/patch"
	"stressTest/pkg/post"
	"stressTest/pkg/put"
	"stressTest/pkg/stress"
	"syscall"
	"time"
)

var (
	duration = time.Minute
)

func init() {
	rps := flag.Int("rps", 1, "the Base of rps")
	resnum := flag.Int("resnum", 10, "the num of res prepare for action such as : put patch and so on")
	period := flag.Duration("duration", time.Minute, "test duration time")
	// verbose := flag.Bool("verbose", true, "add more exclamations")
	flag.Parse()
	stress.SingleResNum = *resnum
	stress.RpsBase = *rps
	duration = *period
	log.Println("the base of rps is", *rps)
	log.Println("the num of res is", *resnum)
	log.Println("duration is", *period)
}

func main() {
	rps()
	log.Println("stress test finished")
}

func rps() {
	resRatio := map[string]map[string]int{
		"POST":  {"podtemplate": 4, "ns": 4, "pv": 4, "cm": 4, "ep": 4, "limits": 4, "pvc": 4, "rc": 4, "quota": 4, "secret": 4, "sa": 4, "svc": 4, "deploy": 4, "rs": 4, "sts": 4, "cj": 4},
		"PATCH": {"podtemplate": 2, "ns": 2, "pv": 2, "cm": 2, "ep": 2, "limits": 2, "pvc": 2, "rc": 2, "quota": 2, "secret": 2, "sa": 2, "svc": 2, "deploy": 2, "rs": 2, "sts": 2, "cj": 2},
		// "PUT":    {"podtemplate": 8, "ns": 8, "pv": 8, "cm": 8, "ep": 8, "limits": 8, "pvc": 8, "rc": 8, "quota": 8, "secret": 8, "sa": 8, "svc": 8, "deploy": 8, "rs": 8, "sts": 8, "cj": 8},
		"PUT":    {"podtemplate": 1, "ns": 1, "pv": 1, "cm": 1, "ep": 1, "limits": 1, "pvc": 1, "rc": 1, "quota": 1, "secret": 1, "sa": 1, "svc": 1, "deploy": 1, "rs": 1, "sts": 1, "cj": 1},
		"GET":    {"podtemplate": 25, "ns": 25, "pv": 25, "cm": 25, "ep": 25, "limits": 25, "pvc": 25, "rc": 25, "quota": 25, "secret": 25, "sa": 25, "svc": 25, "deploy": 25, "rs": 25, "sts": 25, "cj": 25, "no": 25, "po": 25},
		"LIST":   {"podtemplate": 6, "ns": 6, "pv": 6, "cm": 6, "ep": 6, "limits": 6, "pvc": 6, "rc": 6, "quota": 6, "secret": 6, "sa": 6, "svc": 6, "deploy": 6, "rs": 6, "sts": 6, "cj": 6, "no": 6, "po": 6},
		"DELETE": {"podtemplate": 1, "ns": 1, "pv": 1, "cm": 1, "ep": 1, "limits": 1, "pvc": 1, "rc": 1, "quota": 1, "secret": 1, "sa": 1, "svc": 1, "deploy": 1, "rs": 1, "sts": 1, "cj": 1},
	}
	stress.RpsWithPercent(resRatio, duration)
}
func postRps() {

	resRatio := map[string]map[string]int{
		"POST": {"cm": 1},
	}
	stress.RpsWithPercent(resRatio, time.Second*60)
}
func postTest() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	done := make(chan bool, 1)
	go func() {
		sig := <-sigs
		done <- true
		fmt.Println()
		fmt.Println(sig)
		cancel()
		log.Println("received interrupt , run aftercare program ,and clearing")
		log.Println("start clear by interput conn : ")
	}()
	rl := []string{"sa", "svc",
		"ds", "deploy", "rs", "sts", "no", "pv",
		"cm", "ep", "limits", "pvc", "po", "podtemplate",
		"rc", "cj", "job"}
	concurrency_list := []int{3, 6, 9, 15, 30, 60}
	anno_num_list := []int{100, 200, 300, 400}
	for _, an := range anno_num_list {
		for _, cn := range concurrency_list {
			for _, v := range rl {
				select {
				case <-done:
					log.Println("pra stop")
					return
				default:
					s := post.NewStress(-1, cn, an, "myx-test", time.Minute)
					s.Res = v
					s.RpsPerConn = 100
					s.Run(ctx)
				}
			}
		}
	}
}
func clearSingle(res string) {
	if res == "ns" {
		delete.DeleteNameSpace(res, "myx-test", "env=test", "Bearer "+defs.Token)
	} else {
		delete.ClearPost(res, "myx-test", "env=test", "Bearer "+defs.Token)
	}
}
func patchTest(res string) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	concurrency_list := []int{3, 6, 9, 15, 30, 60, 90, 150, 300}
	anno_num_list := []int{0, 100, 200, 300, 400}
	for _, an := range anno_num_list {
		for _, cn := range concurrency_list {
			s := patch.NewStress(-1, cn, an, "myx-test", time.Minute)
			s.Res = res
			s.Run(ctx)
			time.Sleep(time.Second * 40)
		}
	}
}
func putTest(res string) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	concurrency_list := []int{3, 6, 9, 15, 30, 60, 90, 150, 300}
	anno_num_list := []int{0, 100, 200, 300, 400}
	for _, an := range anno_num_list {
		for _, cn := range concurrency_list {
			s := put.NewStress(-1, cn, an, "myx-test", time.Minute)
			s.Res = res
			s.Run(ctx)
			time.Sleep(time.Second * 40)
		}
	}
}
func deleteTest(res string) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	s := delete.NewStress(10, "myx-test", time.Second*100)
	s.Res = res
	s.Run(ctx)
}

func clear() {
	delete.ClearAll("myx-test", "env=test", "Bearer "+defs.Token)
}

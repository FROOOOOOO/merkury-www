package stress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"stressTest/client"
	"stressTest/config"
	"stressTest/defs"
	del "stressTest/pkg/delete"
	create "stressTest/pkg/post"
	"stressTest/util"
	"sync"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
)

type empty struct{}

var (
	funcs = map[string]func(*sync.WaitGroup, context.Context, string, int){
		"POST":   Post,
		"PATCH":  Patch,
		"DELETE": Delete,
		"PUT":    Put,
		"GET":    Get,
		"LIST":   List,
	}
	Resindex     map[string][]string
	GetPodRes    []string
	SingleResNum int
	Rescounter   map[string]int
	usedres      map[string]empty
	RpsBase      int
)

func init() {
	Rescounter = make(map[string]int)
	initResindex()
}
func initResindex() {
	log.Println("in update Resindex")
	Resindex = make(map[string][]string)
	for _, k := range defs.Reslist {
		Resindex[k] = util.GetResList(k)
	}
	log.Println("update Resindex complete!")

}

func RpsWithPercent(resRatio map[string]map[string]int, duration time.Duration) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	err := prepareRes(sigs, resRatio)
	if err != nil {
		log.Println("err", err)
		return
	}
	defer clear()
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(duration))
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		sig := <-sigs
		wg.Add(1)
		done <- true
		fmt.Println()
		fmt.Println(sig)
		cancel()
		time.Sleep(time.Second)
		log.Println("received interrupt, run aftercare program, and cleaning")
		wg.Done()
	}()

	for action, v := range resRatio {
		for res, rps := range v {
			select {
			case <-done:
				log.Println("programmer stop")
				return
			default:
				if rps == 0 {
					continue
				}
				wg.Add(1)
				log.Println("start goroutine in:", action, res, rps*RpsBase, "ticker time is", time.Second/time.Duration(rps*RpsBase))
				go funcs[action](wg, ctx, res, rps*RpsBase)
			}
		}
	}
	wg.Wait()
}

func prepareRes(sigs chan os.Signal, resRatio map[string]map[string]int) error {
	log.Println("prepare res, creating res")
	usedres = make(map[string]empty)
	if len(resRatio) >= 1 {
		for action, resmap := range resRatio {
			if action == "POST" {
				continue
			}
			for res := range resmap {
				if res == "po" || res == "no" {
					continue
				}
				usedres[res] = empty{}
			}
		}
		for res := range usedres {
			if list, ok := Resindex[res]; ok {
				select {
				case <-sigs:
					log.Println("when creat res received interrupt , run aftercare program ,and clearing")
					clear()
					return errors.New("receive interupt when create res")
				default:
					if len(list) > SingleResNum {
						log.Println("res ,", res, "num is sufficient num is:", len(list))
						Rescounter[res] = len(list)
					} else {
						log.Println("creating res", res, "num:", SingleResNum-len(list))
						create.CreateRes(config.GetDefultNameSpace(), res, SingleResNum-len(list))
						log.Println("creating res", res, "num:", SingleResNum-len(list), "complete")
						Rescounter[res] = 200
					}
				}
			}
		}
	}
	log.Println("get or create res list complete")
	initResindex()
	return nil
}

func prepareGetRes(kind string) []string {
	client := client.GetClientWithoutReuse(false)
	request := "https://192.168.12.127:6443/api/v1/" + kind
	req, err := http.NewRequest("GET", request, nil)
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Panicln("do request has err", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("read response has err", err)
	}
	// log.Println("body", string(body))
	nslist := v1.NamespaceList{}
	err = json.Unmarshal(body, &nslist)
	if err != nil {
		log.Println("unmarshal err", err)
	}
	resu := []string{}
	for _, item := range nslist.Items {
		resu = append(resu, item.Name)
	}
	return resu
}

func clear() {
	log.Println("start clear program")
	wg := &sync.WaitGroup{}
	for res := range usedres {
		lock.Lock()
		wg.Add(1)
		go del.Delete(wg, 0, Rescounter[res], res)
		lock.Unlock()
	}
	wg.Wait()
	log.Println("clear rps test res complete!")
}

// func verify(actionRatio map[string]float64, resRatio map[string]map[string]float64) error {
// 	log.Println("verfying data legality")
// 	t1 := 0.0
// 	for action, v := range actionRatio {
// 		t1 += v
// 		t2 := 0.0
// 		for _, rat := range resRatio[action] {
// 			t2 += rat
// 		}
// 		if t2 > 1.0 {
// 			return errors.New("res ratio greater than 1")
// 		}
// 	}
// 	if t1 > 1.0 {
// 		return errors.New("action ratio greater than 1")
// 	}
// 	return nil
// }

package delete

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"stressTest/client"
	"stressTest/defs"
	"stressTest/ioinfo"
	"stressTest/util"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
)

type stress struct {
	defs.Meta
	defs.Config
	clientSet   []*http.Client
	ConnSend    []int
	ConnRecv    []int
	ConnSendNum []int
}

func NewStress(conn int, ns string, duration time.Duration) *stress {
	if conn < 1 {
		log.Fatal("connection num is less than one")
	}
	s := &stress{
		Config: defs.Config{
			Conn:          conn,
			Duration:      duration,
			Action:        "DELETE",
			LabelSelector: "env=test",
			Auth:          "Bearer " + defs.Token,
		},
		Meta: defs.Meta{
			Namespace: ns,
		},
	}
	s.initStress()
	return s
}
func (s *stress) initStress() {
	s.ConnSend = make([]int, s.Conn)
	s.ConnRecv = make([]int, s.Conn)
	s.ConnSendNum = make([]int, s.Conn)
	s.clientSet = client.ClientSetWithReuse(s.Conn)
}
func (s *stress) Info() (string, string, string, int, int, time.Duration, []int, []int, []int) {
	return s.Res, s.Namespace, s.Action, s.Conn, s.Anntation, s.Duration, s.ConnSend, s.ConnRecv, s.ConnSendNum
}
func (s *stress) Run(ctx context.Context) {
	start := time.Now()
	s.run()
	ioinfo.WriteInfo(start, s)
}

func (s *stress) start(ctx context.Context, wg *sync.WaitGroup, id int, reslist []string, httpClient *http.Client) {
	fmt.Println(reslist)
	defer wg.Done()
	for _, v := range reslist {
		select {
		case <-ctx.Done():
			return
		default:
			s.ConnSendNum[id]++
			s.delete(id, v, httpClient)
		}
	}
	// fmt.Println("3")
}

func (s *stress) run() {

	// defer client.PutClientSet(s.clientSet)
	list := s.getResList()
	// fmt.Println(list)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(s.Duration))
	defer cancel()
	wg := &sync.WaitGroup{}
	if len(list) <= s.Conn {
		wg.Add(1)
		go s.start(ctx, wg, 0, list, s.clientSet[0])
	} else {
		avg := len(list) / s.Conn
		remain := len(list) % s.Conn
		for i := 0; i < s.Conn; i++ {
			wg.Add(1)
			// fmt.Println("1")
			go s.start(ctx, wg, i, list[i*avg:(i+1)*avg], s.clientSet[i])
		}
		wg.Add(1)
		// fmt.Println("2")
		go s.start(ctx, wg, 0, list[len(list)-remain:], s.clientSet[0])
	}
	wg.Wait()
}

func (s *stress) getResList() []string {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	_, _, request := util.GetBasic(s.Res, s.Namespace)
	req, err := http.NewRequest("GET", request+"?labelSelector="+s.LabelSelector, nil)
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", s.Auth)
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
	res := []string{}
	for _, item := range nslist.Items {
		res = append(res, item.Name)
	}
	return res
}
func (s *stress) delete(id int, resName string, client *http.Client) {
	_, _, request := util.GetBasic(s.Res, s.Namespace)
	req, err := http.NewRequest("DELETE", request+"/"+resName, nil)
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", s.Auth)
	req.Header.Set("Content-Type", "application/json")
	log.Println("DELETE", req.URL.String())
	reqout, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		log.Println("parse to request out err", err)
	}
	s.ConnSend[id] += len(reqout)
	resp, err := client.Do(req)
	if err != nil {
		log.Panicln("do request has err", err)
	}
	defer resp.Body.Close()
	repout, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Println("parse to reponse out err", err)
	}
	s.ConnRecv[id] += len(repout)
	log.Println("resp: ", resp.Status, resp.Request.Method, resp.Request.URL)
}

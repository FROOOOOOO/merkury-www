package patch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
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

const (
	JsonPatch           = "application/json-patch+json"
	MergePatch          = "application/merge-patch+json"
	StrategicMergePatch = "application/strategic-merge-patch+json"
)

type stress struct {
	defs.Meta
	defs.Config
	clientSet   []*http.Client
	ConnSend    []int
	ConnRecv    []int
	ConnSendNum []int
}

func NewStress(num, conn, anno int, ns string, duration time.Duration) *stress {
	if conn < 1 {
		log.Fatal("connection num is less than one")
	}
	if anno < 0 {
		anno = 0
		log.Println("anno is less than 0, change it to 0")
	}
	s := &stress{
		Config: defs.Config{
			Conn:          conn,
			Num:           num,
			Duration:      duration,
			Anntation:     anno,
			Action:        "PATCH",
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
func (s *stress) Run(ctx context.Context) {
	start := time.Now()
	s.run()
	ioinfo.WriteInfo(start, s)
}
func (s *stress) Info() (string, string, string, int, int, time.Duration, []int, []int, []int) {
	return s.Res, s.Namespace, s.Action, s.Conn, s.Anntation, s.Duration, s.ConnSend, s.ConnRecv, s.ConnSendNum

}
func (s *stress) startWithDeadline(ctx context.Context, wg *sync.WaitGroup, id int, reslist []string, httpClient *http.Client) {
	s.start(ctx, wg, math.MaxInt64, id, reslist, httpClient)
}
func (s *stress) start(ctx context.Context, wg *sync.WaitGroup, num, id int, reslist []string, httpClient *http.Client) {
	defer wg.Done()
	spos := rand.Int() % len(reslist)
	for i := 0; i < num; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			s.ConnSendNum[id]++
			s.patch(id, reslist[spos], httpClient)
			spos = (spos + 1) % len(reslist)
		}
	}
}

func (s *stress) run() {

	// defer client.PutClientSet(s.clientSet)
	list := s.getResList()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(s.Duration))
	defer cancel()
	wg := &sync.WaitGroup{}
	if s.Num < 0 {
		for i := 0; i < s.Conn; i++ {
			wg.Add(1)
			go s.startWithDeadline(ctx, wg, i, list, s.clientSet[i])
		}
	} else if s.Num > 0 {
		avg := s.Num / s.Conn
		remain := s.Num % s.Conn
		for i := 0; i < s.Conn; i++ {
			wg.Add(1)
			if i < remain {
				go s.start(ctx, wg, avg+1, i, list, s.clientSet[i])
			} else {
				go s.start(ctx, wg, avg, i, list, s.clientSet[i])
			}
		}
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
func (s *stress) patch(id int, resName string, client *http.Client) {
	_, _, request := util.GetBasic(s.Res, s.Namespace)
	anno := util.GetPatchAnnotations(s.Anntation)
	req, err := http.NewRequest("PATCH", request+"/"+resName, bytes.NewBufferString(anno))
	log.Println("PATCH", req.URL.String())
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", s.Auth)
	req.Header.Set("Content-Type", StrategicMergePatch)
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

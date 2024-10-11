package post

import (
	"bytes"
	"context"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httputil"
	"strconv"
	"stressTest/client"
	"stressTest/defs"
	"stressTest/ioinfo"
	"stressTest/util"
	"strings"
	"sync"
	"time"
)

var (
	Debug bool
)

type stress struct {
	defs.Meta
	defs.Config
	RpsPerConn  int
	clientSet   []*http.Client
	ConnSend    []int
	ConnRecv    []int
	ConnSendNum []int
}

func NewStress(num, conn, antNum int, ns string, duration time.Duration) *stress {
	if num > 0 && num < conn {
		panic("num must greater or equal than conn")
	}
	if conn < 1 {
		panic("conn must greater than 0")
	}
	if antNum < 0 {
		panic("anntation number must greater or equal than zero ")
	}

	res := &stress{
		Meta: defs.Meta{
			Namespace: ns,
		},
		Config: defs.Config{
			Conn:          conn,
			Num:           num,
			Duration:      duration,
			Action:        "POST",
			Anntation:     antNum,
			LabelSelector: "env=test",
			Auth:          "Bearer " + defs.Token,
		},
	}
	res.initStress()
	return res
}
func CreateRes(ns, res string, num int) {
	Debug = true
	s := NewStress(num, 1, 0, ns, time.Hour)
	s.clientSet = client.ClientSetWithOutReuse(s.Conn)
	s.Res = res
	s.RpsPerConn = 50
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(s.Duration))
	defer cancel()
	s.run(ctx, context.TODO())
	Debug = false
}
func (s *stress) Run(ctx context.Context) {
	deadctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(s.Duration))
	defer cancel()
	start := time.Now()
	log.Println("start post conn : ", s.Conn, ", annotation : ", s.Anntation, "res:", s.Res)
	s.run(deadctx, ctx)
	select {
	case <-ctx.Done():
		log.Println("programm has been interupted res: ", s.Res, "conn", s.Conn)
		log.Println("now is clearing the res")
		time.Sleep(time.Second * 2)
		s.Clear()
		log.Println("clear complete")
		return
	default:
		ioinfo.WriteInfo(start, s)
		time.Sleep(time.Minute * 1)
		start = time.Now()
		log.Println("start clear conn : ", s.Conn, ", annotation : ", s.Anntation, "res:", s.Res)
		s.Clear()
		log.Println("clear res complete")
		end := time.Now()
		s.Duration = end.Sub(start)
		ioinfo.WriteInfo(start, s)
		time.Sleep(time.Minute)
	}

}
func (s *stress) Info() (string, string, string, int, int, time.Duration, []int, []int, []int) {
	return s.Res, s.Namespace, s.Action, s.Conn, s.Anntation, s.Duration, s.ConnSend, s.ConnRecv, s.ConnSendNum
}

func (s *stress) initStress() {
	s.ConnSend = make([]int, s.Conn)
	s.ConnRecv = make([]int, s.Conn)
	s.ConnSendNum = make([]int, s.Conn)
	s.clientSet = client.ClientSetWithReuse(s.Conn)
}
func (s *stress) run(ctx, cal context.Context) {

	// defer client.PutClientSet(s.clientSet)
	wg := &sync.WaitGroup{}
	if s.Num < 0 {
		for i := 0; i < s.Conn; i++ {
			wg.Add(1)
			go s.startWithDeadline(ctx, cal, wg, i, s.clientSet[i])
		}
	} else if s.Num > 0 {
		avg := s.Num / s.Conn
		remain := s.Num % s.Conn
		for i := 0; i < s.Conn; i++ {
			wg.Add(1)
			if i < remain {
				go s.start(ctx, cal, wg, avg+1, i, s.clientSet[i])
			} else {
				go s.start(ctx, cal, wg, avg, i, s.clientSet[i])
			}
		}
	}
	wg.Wait()
}

func (s *stress) startWithDeadline(ctx, cal context.Context, wg *sync.WaitGroup, id int, httpClient *http.Client) {
	s.start(ctx, cal, wg, math.MaxInt64, id, httpClient)
}
func (s *stress) start(ctx, cal context.Context, wg *sync.WaitGroup, num, id int, httpClient *http.Client) {
	defer wg.Done()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	tick := time.NewTicker(time.Second / time.Duration(s.RpsPerConn))
	defer tick.Stop()
	for i := 0; i < num; i++ {
		select {
		case <-ctx.Done():
			return
		case <-cal.Done():
			return
		case <-tick.C:
			s.ConnSendNum[id]++
			wgr.Add(1)
			s.post(wgr, httpClient, i, id)
		}
	}
}

func (s *stress) post(wg *sync.WaitGroup, httpClient *http.Client, seq, id int) {
	defer wg.Done()
	data, url := util.GetPostDataAndUrl(s.Res, s.Namespace, s.Anntation, seq, id)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	// if Debug {
	// 	log.Println("POST", req.URL.String(), "test-", seq, "-", id)
	// }
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", s.Auth)
	req.Header.Set("Content-Type", "application/json")
	reques, err := httputil.DumpRequestOut(req, true)
	s.ConnSend[id] += len(reques)
	if err != nil {
		log.Println("DumpRequestOut err:", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("err:", err)
		return
	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp: ", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	respo, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Println("DumpResponse err:", err)
	}
	s.ConnRecv[id] += len(respo)
	io.Copy(io.Discard, resp.Body)
}

func (s *stress) Clear() {
	s.Action = "DELETE"
	connSendNum := make([]int, len(s.ConnSendNum))
	copy(connSendNum, s.ConnSendNum)
	s.initStress()
	s.runDel(connSendNum)
}

func (s *stress) runDel(connSendNum []int) {
	wg := &sync.WaitGroup{}
	f := func(id, num int, client *http.Client) {
		for i := 0; i < num; i++ {
			resName := "test-" + strings.ToLower(util.Res2kind(s.Res)) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(id)
			s.ConnSendNum[id]++
			s.delete(id, resName, client)
		}
		wg.Done()
	}
	for i, v := range connSendNum {
		wg.Add(1)
		go f(i, v, s.clientSet[i])
	}
	time.Sleep(time.Millisecond * 100)
	wg.Wait()
}

func (s *stress) delete(id int, resName string, client *http.Client) {
	_, _, request := util.GetBasic(s.Res, s.Namespace)
	req, err := http.NewRequest("DELETE", request+"/"+resName, nil)
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", s.Auth)
	req.Header.Set("Content-Type", "application/json")
	// log.Println("DELETE", req.URL.String())
	reqout, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		log.Println("parse to request out err", err)
	}
	s.ConnSend[id] += len(reqout)
	resp, err := client.Do(req)
	if err != nil {
		log.Println("do request has err", err)
		return
	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp: ", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	repout, err := httputil.DumpResponse(resp, true)
	if err != nil {
		log.Println("parse to reponse out err", err)
	}
	s.ConnRecv[id] += len(repout)
	io.Copy(io.Discard, resp.Body)
}

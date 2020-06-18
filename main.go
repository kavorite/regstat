package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kavorite/smooch"
	http "github.com/valyala/fasthttp"
	gq "gopkg.in/goquery.v1"
)

type DOB struct {
	Month, Day, Year string
}

type Address struct {
	House, Street, Zip string
}

type Contact struct {
	Surname string
	Address
	DOB
}

type Status struct {
	Registration, Party, Balloting, PollingSite string
}

func (contact Contact) Status(client *http.Client) (*Status, error) {
	data := url.Values{}
	data.Add("v[lname]", contact.Surname)
	data.Add("v[no]", contact.House)
	data.Add("v[sname]", contact.Street)
	data.Add("v[zip]", contact.Zip)
	data.Add("v[dobm]", contact.Month)
	data.Add("v[dobd]", contact.Day)
	data.Add("v[doby]", contact.Year)
	req := http.AcquireRequest()
	req.SetBodyString(data.Encode())
	req.SetRequestURI("https://www.monroecounty.gov/etc/voter/index.php")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/x-www-form-urlencoded")
	req.Header.SetUserAgent(
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
			"AppleWebKit/537.36 (KHTML, like Gecko) " +
			"Safari/537.36")
	rsp := http.AcquireResponse()
	defer http.ReleaseResponse(rsp)
	client.Dial = http.DialFunc(func(addr string) (conn net.Conn, err error) {
		for i := 0; i < 3; i++ {
			conn, err = net.Dial("tcp", addr)
			if err != nil {
				continue
			}
			return conn, nil
			time.Sleep(time.Second * 4)
		}
		return nil, err
	})
	err := client.Do(req, rsp)
	http.ReleaseRequest(req)
	if err != nil {
		return nil, err
	}
	doc, err := gq.NewDocumentFromReader(bytes.NewReader(rsp.Body()))
	if err != nil {
		return nil, err
	}
	reg := doc.Find("div#voter")
	if reg.Length() < 1 {
		return nil, fmt.Errorf("markup incomplete")
	}
	registration := reg.Get(0).Attr[1].Val
	reg = reg.Find("div > span")
	if reg.Length() < 2 {
		return nil, fmt.Errorf("markup incomplete")
	}
	party := strings.TrimSpace(reg.Get(0).NextSibling.Data)
	ballot := strings.TrimSpace(reg.Get(1).NextSibling.Data)
	poll := doc.Find("div#poll").Children()
	if poll.Length() < 4 {
		return nil, fmt.Errorf("markup incomplete")
	}
	pollingSite := strings.TrimSpace(poll.Get(2).NextSibling.Data)
	pollingSite += ", " + strings.TrimSpace(poll.Get(3).NextSibling.Data)
	pollingSite = strings.Title(strings.ToLower(pollingSite))
	rtn := &Status{
		Party:        party,
		Balloting:    ballot,
		PollingSite:  pollingSite,
		Registration: registration,
	}
	return rtn, nil
}

type RowSchema struct {
	Surname, House, Street, Zip, DOB int
}

func (schema RowSchema) ReadContact(row []string) (*Contact, error) {
	triplet := strings.SplitN(row[schema.DOB], "/", 3)
	if len(triplet) != 3 {
		return nil, fmt.Errorf("malformed DoB")
	}
	return &Contact{
		Surname: row[schema.Surname],
		Address: Address{
			House:  row[schema.House],
			Street: row[schema.Street],
			Zip:    row[schema.Zip],
		},
		DOB: DOB{triplet[0], triplet[1], triplet[2]},
	}, nil
}

var (
	cdlRowSchema = RowSchema{1, 5, 6, 13, 27}
	boeRowSchema = RowSchema{6, 9, 10, 14, 15}
)

func worker(in <-chan []string, out chan<- []string, schema RowSchema, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	client := &http.Client{}
	for head := range in {
		contact, err := schema.ReadContact(head)
		if err != nil {
			continue
		}
		stat, err := contact.Status(client)
		if err != nil {
			if err.Error() == "markup incomplete" {
				continue
			} else {
				panic(err)
			}
		}
		tail := []string{
			stat.Registration,
			stat.Party,
			stat.Balloting,
			stat.PollingSite,
		}
		head = append(head, tail...)
		out <- head
	}
}

func main() {
	var (
		schema    string
		rowSchema RowSchema
	)
	flag.StringVar(&schema, "schema", "cdl", "row schema preset")
	flag.Parse()
	switch strings.ToLower(schema) {
	case "boe":
		rowSchema = boeRowSchema
	case "cdl":
		rowSchema = cdlRowSchema
	default:
		panic("invalid schema preset")
	}
	wg := sync.WaitGroup{}
	istrm, ostrm := csv.NewReader(os.Stdin), csv.NewWriter(os.Stdout)
	workerc := 32
	i, o := make(chan []string, 4*workerc), make(chan []string, 4*workerc)
	for n := 1; n <= workerc; n++ {
		go worker(i, o, rowSchema, &wg)
	}
	go func() {
		wg.Wait()
		close(o)
	}()
	progress := make(chan int)
	done := make(chan struct{})
	start := time.Now()
	rows, err := istrm.ReadAll()
	if err != nil && err != io.EOF {
		panic(err)
	}
	go func() {
		s := int64(time.Second)
		timeScale := smooch.ScaleOf(smooch.Scale{
			{3600 * s, "hr"},
			{60 * s, "min"},
		}...)
		pp := float64(0)
		for k := range progress {
			g := len(rows)
			p := 100 * float64(k) / float64(g)
			now := time.Now()
			if pp == 0 || (p-pp) >= 1 {
				elapsed := float64(now.Sub(start)) / float64(time.Second)
				rate := float64(k) / elapsed
				eta := timeScale.Format(int64(float64(g-k)/rate*float64(s)), s, true)
				fmt.Fprintf(os.Stderr, "[%s] %06.2f%%: %d/%dit. (%.2fit./s; apx. %s remaining)\n",
					now.Format("15:04:05"), p, k, g, rate, eta)
				pp = p
			}
		}
	}()
	go func() {
		k := 0
		ticker := time.NewTicker(time.Second).C
		progress <- k
		for row := range o {
			if err := ostrm.Write(row); err != nil {
				panic(err)
			}
			k++
			select {
			case <-ticker:
				progress <- k
			default:
				continue
			}
		}
		close(progress)
		done <- struct{}{}
	}()
	max := 0
	indices := []int{
		rowSchema.DOB,
		rowSchema.House,
		rowSchema.Street,
		rowSchema.Surname,
		rowSchema.Zip,
	}
	for i := range indices {
		if i > max {
			max = i
		}
	}
	for _, row := range rows {
		if len(row) > max {
			i <- row
		}
	}
	close(i)
	<-done
}

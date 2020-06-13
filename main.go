package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

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

func (contact Contact) Status() (*Status, error) {
	data := url.Values{}
	data.Add("v[lname]", contact.Surname)
	data.Add("v[no]", contact.House)
	data.Add("v[sname]", contact.Street)
	data.Add("v[zip]", contact.Zip)
	data.Add("v[dobm]", contact.Month)
	data.Add("v[dobd]", contact.Day)
	data.Add("v[doby]", contact.Year)
	rsp, err := http.PostForm("https://www.monroecounty.gov/etc/voter/index.php", data)
	if err != nil {
		return nil, err
	}
	doc, err := gq.NewDocumentFromResponse(rsp)
	if err != nil {
		return nil, err
	}
	reg := doc.Find("div#voter")
	registration := reg.Get(0).Attr[1].Val
	reg = reg.Find("div > span")
	party := strings.TrimSpace(reg.Get(0).NextSibling.Data)
	ballot := strings.TrimSpace(reg.Get(1).NextSibling.Data)
	poll := doc.Find("div#poll").Children()
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
	for head := range in {
		contact, err := schema.ReadContact(head)
		if err != nil {
			continue
		}
		stat, err := contact.Status()
		if err != nil {
			panic(err)
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
	workerc := 64
	i, o := make(chan []string, workerc), make(chan []string, workerc)
	for n := 1; n <= workerc; n++ {
		go worker(i, o, rowSchema, &wg)
	}
	go func() {
		wg.Wait()
		close(o)
	}()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		k := 0
		for row := range o {
			if err := ostrm.Write(row); err != nil {
				panic(err)
			}
			k++
			elapsed := float64(time.Now().Sub(start)) / float64(time.Second)
			rate := float64(k) / elapsed
			fmt.Fprintf(os.Stderr, "%dit. (%.2fit./s)\r", k, rate)
		}
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
	for {
		row, err := istrm.Read()
		if err != nil {
			if err == io.EOF {
				close(i)
				wg.Wait()
				<-done
				ostrm.Flush()
				return
			}
			panic(err)
		}
		if len(row) > max {
			i <- row
		}
	}
}

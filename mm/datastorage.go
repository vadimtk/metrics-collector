package mm

import "fmt"
import "time"
import "gopkg.in/mgo.v2"

import (
	log "github.com/Sirupsen/logrus"
)

type DataStorage struct {
	url string
}

type MongoRecord struct {
	Ts     time.Time
	Name   string
	Values []float64
}

func (ds *DataStorage) Write(service string, data *Report) error {
	log.Debug("write data")
	fmt.Printf("data: %+v\n", data)
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB("metrics").C("data")

	//recs := []*MongoRecord{}
	//recs := make([]MongoRecord, len(data.Stats[0].Stats))

	i := 0
	for key, value := range data.Stats[0].Stats {
		//fmt.Printf("Stat: %d => %+v\n",key,value)
		rec := &MongoRecord{}
		rec.Ts = data.Ts
		rec.Name = key
		rec.Values = value.Vals
		err := c.Insert(rec)
		if err != nil {
			log.Fatal(err)
		}
		//recs = append(recs,rec)
		i++
	}
	//x := c.Bulk();
	//x.Unordered();
	//x.Insert(recs);
	//_, err = x.Run()
	return nil
}

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
	Avg    float64
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

	recs := make([]interface{}, len(data.Stats[0].Stats))

	i := 0
	for key, value := range data.Stats[0].Stats {
		//fmt.Printf("Stat: %d => %+v\n",key,value)
		rec := &MongoRecord{}
		rec.Ts = data.Ts
		rec.Name = key
		rec.Values = value.Vals
		rec.Avg = value.Avg
		recs[i] = rec
		i++
	}

	err=c.Insert(recs...);
	if err != nil {
		panic(err)
	}
	return nil
}

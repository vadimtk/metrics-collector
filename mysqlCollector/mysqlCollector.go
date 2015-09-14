package mysqlCollector

import (
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"../mm"
	"../mysql"
	log "github.com/Sirupsen/logrus"
)

var (
	accessDenied = errors.New("Access Denied")
	networkError = errors.New("Network error")
)

type MySQLCollector struct {
	url            string
	conn           mysql.Connector
	config         *Config
	tickChan       <-chan time.Time
	collectionChan chan *mm.Collection
	connectedChan  chan bool
}

func NewMysqlCollector(url string) *MySQLCollector {
	m := &MySQLCollector{
		url:           url,
		conn:          mysql.NewConnection(url),
		connectedChan: make(chan bool, 1),
		config:        &Config{Status: GlobalMySQLStatus, InnoDB: []string{"%"}},
	}
	return m
}

func (m *MySQLCollector) Start(tickChan <-chan time.Time, collectionChan chan *mm.Collection) error {
	m.tickChan = tickChan
	m.collectionChan = collectionChan
	go m.run()

	return nil
}

func (m *MySQLCollector) connect() {
	log.Debug("connect:call")
	defer func() {
		if err := recover(); err != nil {
			log.Error("MySQL connection crashed: ", err)
		}
		log.Debug("connect:return")
	}()
	// Close/release previous connection, if any.
	m.conn.Close()

	// Try forever to connect to MySQL...
	for {
		log.Debug("connect:try")
		if err := m.conn.Connect(1); err != nil {
			log.Warn(err)
			continue
		}
		log.Info("Connected")

		m.setGlobalVars()

		// Tell run() goroutine that it can try to collect metrics.
		// If connection is lost, it will call us again.
		log.Debug("connectedChan:true")
		m.connectedChan <- true
		log.Debug("connectedChan:sent")
		// Add the instance only when we have a connection. Otherwise, mrm.Add will fail
		return
	}
}

func (m *MySQLCollector) setGlobalVars() {
	log.Debug("setGlobalVars:call")
	defer log.Debug("setGlobalVars:return")

	// Set global vars we need.  If these fail, that's ok: they won't work,
	// but don't let that stop us from collecting other metrics.
	/*
		if len(m.config.InnoDB) > 0 {
			log.Debug("setGlobalVars:InnoDB config")
			for _, module := range m.config.InnoDB {
				sql := "SET GLOBAL innodb_monitor_enable = '" + module + "'"
				if _, err := m.conn.DB().Exec(sql); err != nil {
					errMsg := fmt.Sprintf("Cannot collect InnoDB stats because '%s' failed: %s", sql, err)
					log.Error(errMsg)
					m.config.InnoDB = []string{}
					break
				}
			}
		}
	*/
}

func (m *MySQLCollector) run() {
	log.Debug("run:call")
	defer func() {
		if err := recover(); err != nil {
			log.Error("MySQL monitor crashed: ", err)
		}
		m.conn.Close()
		log.Debug("run:return")
	}()

	connected := false
	go m.connect()

	for {

		select {
		case now := <-m.tickChan:
			log.Debug("run:collect:start")
			if !connected {
				log.Debug("run:collect:disconnected")
				continue
			}

			c := &mm.Collection{
				Ts:      now.UTC().Unix(),
				Metrics: []mm.Metric{},
			}

			// Start timing the collection.  If must take < collectLimit else
			// it's discarded.
			conn := m.conn.DB()

			// SHOW GLOBAL STATUS
			if err := m.GetShowStatusMetrics(conn, c); err != nil {
				if m.collectError(err) == networkError {
					connected = false
					continue
				}
			}
			//fmt.Printf("global : %+v\n",c.Metrics)

			// SELECT NAME, ... FROM INFORMATION_SCHEMA.INNODB_METRICS
			if len(m.config.InnoDB) > 0 {
				if err := m.GetInnoDBMetrics(conn, c); err != nil {
					switch m.collectError(err) {
					case accessDenied:
						m.config.InnoDB = []string{}
					case networkError:
						connected = false
						continue
					}
				}
			}

			// It is possible that collecting metrics will stall for many
			// seconds for some reason so even though we issued captures 1 sec in
			// between, we actually got 5 seconds between results and as such we
			// might be showing huge spike.
			// To avoid that, if the time to collect metrics is >= collectLimit
			// then warn and discard the metrics.

			// Send the metrics to an mm.Aggregator.
			if len(c.Metrics) > 0 {
				select {
				case m.collectionChan <- c:
				case <-time.After(500 * time.Millisecond):
					// lost collection
					log.Debug("Lost MySQL metrics; timeout spooling after 500ms")
				}
			} else {
				log.Debug("run:no metrics") // shouldn't happen
			}

			log.Debug("run:collect:stop")
		case connected = <-m.connectedChan:
			log.Debug("run:connected:true")
		}
	}
}

// --------------------------------------------------------------------------
// SHOW STATUS
// --------------------------------------------------------------------------

func (m *MySQLCollector) GetShowStatusMetrics(conn *sql.DB, c *mm.Collection) error {
	log.Debug("GetShowStatusMetrics:call")
	defer log.Debug("GetShowStatusMetrics:return")

	rows, err := conn.Query("SHOW /*!50002 GLOBAL */ STATUS")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var statName string
		var statValue string
		if err = rows.Scan(&statName, &statValue); err != nil {
			return err
		}

		statName = strings.ToLower(statName)
		metricType, ok := m.config.Status[statName]
		if !ok {
			continue // not collecting this stat
		}

		if statValue == "" {
			// Some values aren't set when not applicable,
			// e.g. slave_heartbeat_period on a master.
			continue
		}

		metricValue, err := strconv.ParseFloat(statValue, 64)
		if err != nil {
			log.Warn(fmt.Sprintf("Cannot convert '%s' value '%s' to float: %s", statName, statValue, err))
			delete(m.config.Status, statName) // stop collecting it
			continue
		}

		c.Metrics = append(c.Metrics, mm.Metric{"mysql/" + statName, metricType, metricValue, ""})
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}

// --------------------------------------------------------------------------
// InnoDB Metrics
// http://dev.mysql.com/doc/refman/5.6/en/innodb-metrics-table.html
// https://blogs.oracle.com/mysqlinnodb/entry/get_started_with_innodb_metrics
// --------------------------------------------------------------------------

func (m *MySQLCollector) GetInnoDBMetrics(conn *sql.DB, c *mm.Collection) error {
	log.Debug("GetInnoDBMetrics:call")
	defer log.Debug("GetInnoDBMetrics:return")

	rows, err := conn.Query("SELECT NAME, SUBSYSTEM, COUNT, TYPE FROM INFORMATION_SCHEMA.INNODB_METRICS WHERE STATUS='enabled'")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var statName string
		var statSubsystem string
		var statCount string
		var statType string
		err = rows.Scan(&statName, &statSubsystem, &statCount, &statType)
		if err != nil {
			return err
		}

		metricName := "mysql/innodb/" + strings.ToLower(statSubsystem) + "/" + strings.ToLower(statName)
		metricValue, err := strconv.ParseFloat(statCount, 64)
		if err != nil {
			log.Warn(fmt.Sprintf("Cannot convert '%s' value '%s' to float: %s", metricName, metricValue, err))
			metricValue = 0.0
		}
		var metricType string
		if statType == "value" {
			metricType = "gauge"
		} else {
			metricType = "counter"
		}
		c.Metrics = append(c.Metrics, mm.Metric{metricName, metricType, metricValue, ""})
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQLCollector) collectError(err error) error {
	switch {
	case mysql.MySQLErrorCode(err) == mysql.ER_SPECIFIC_ACCESS_DENIED_ERROR:
		log.Error(fmt.Sprintf("Cannot collect InnoDB stats: %s", err))
		return accessDenied
	}
	switch err.(type) {
	case *net.OpError:
		log.Warn("Lost connection to MySQL:", err)
		return networkError
	}
	log.Warn(err)
	return err
}

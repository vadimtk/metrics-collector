package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"pct"
)

type Query struct {
	Set    string // SET GLOBAL long_query_time=0
	Verify string // SELECT @@long_query_time
	Expect string // 0
}

type Connector interface {
	DB() *sql.DB
	DSN() string
	Connect(tries uint) error
	Close()
	Set([]Query) error
	GetGlobalVarString(varName string) string
	GetGlobalVarNumber(varName string) float64
	Uptime() (uptime int64, err error)
}

type Connection struct {
	dsn             string
	conn            *sql.DB
	backoff         *pct.Backoff
	connectedAmount uint
	connectionMux   *sync.Mutex
}

func NewConnection(dsn string) *Connection {
	c := &Connection{
		dsn:           dsn,
		backoff:       pct.NewBackoff(20 * time.Second),
		connectionMux: &sync.Mutex{},
	}
	return c
}

func (c *Connection) DB() *sql.DB {
	return c.conn
}

func (c *Connection) DSN() string {
	return c.dsn
}

func (c *Connection) Connect(tries uint) error {
	if tries == 0 {
		return nil
	}
	c.connectionMux.Lock()
	defer c.connectionMux.Unlock()
	if c.connectedAmount > 0 {
		// already have opened connection
		c.connectedAmount++
		return nil
	}
	var err error
	var db *sql.DB
	for i := tries; i > 0; i-- {
		// Wait before attempt.
		time.Sleep(c.backoff.Wait())

		// Open connection to MySQL but...
		db, err = sql.Open("mysql", c.dsn)
		if err != nil {
			continue
		}

		// ...try to use the connection for real.
		if err = db.Ping(); err != nil {
			// Connection failed.  Wrong username or password?
			db.Close()
			continue
		}

		// Connected
		c.conn = db
		c.backoff.Success()
		c.connectedAmount++
		return nil
	}

	return fmt.Errorf("Cannot connect to MySQL %s: %s", HideDSNPassword(c.dsn), FormatError(err))
}

func (c *Connection) Close() {
	c.connectionMux.Lock()
	defer c.connectionMux.Unlock()
	if c.connectedAmount == 0 {
		// connection closed already
		return
	}
	c.connectedAmount--
	if c.connectedAmount == 0 && c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) Set(queries []Query) error {
	if c.conn == nil {
		return errors.New("Not connected")
	}
	for _, query := range queries {
		if query.Set != "" {
			if _, err := c.conn.Exec(query.Set); err != nil {
				return err
			}
		}
		if query.Verify != "" {
			got := c.GetGlobalVarString(query.Verify)
			if got != query.Expect {
				return fmt.Errorf(
					"Global variable '%s' is set to '%s' but needs to be '%s'. "+
						"Consult the MySQL manual, or contact Percona Support, "+
						"for help configuring this variable, then try again.",
					query.Verify, got, query.Expect)
			}
		}
	}
	return nil
}

func (c *Connection) GetGlobalVarString(varName string) string {
	if c.conn == nil {
		return ""
	}
	var varValue string
	c.conn.QueryRow("SELECT @@GLOBAL." + varName).Scan(&varValue)
	return varValue
}

func (c *Connection) GetGlobalVarNumber(varName string) float64 {
	if c.conn == nil {
		return 0
	}
	var varValue float64
	c.conn.QueryRow("SELECT @@GLOBAL." + varName).Scan(&varValue)
	return varValue
}

func (c *Connection) Uptime() (uptime int64, err error) {
	if c.conn == nil {
		return 0, fmt.Errorf("Error while getting Uptime(). Not connected to the db: %s", c.DSN())
	}
	// Result from SHOW STATUS includes two columns,
	// Variable_name and Value, we ignore the first one as we need only Value
	var varName string
	c.conn.QueryRow("SHOW STATUS LIKE 'Uptime'").Scan(&varName, &uptime)
	return uptime, nil
}


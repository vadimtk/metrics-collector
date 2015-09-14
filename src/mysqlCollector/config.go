package mysqlCollector

type Config struct {
	Status            map[string]string // SHOW STATUS variables to collect, case-sensitive
	InnoDB            []string          // SET GLOBAL innodb_monitor_enable="<value>"
}

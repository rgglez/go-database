package database

import (
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

//-----------------------------------------------------------------------------

// Specific MySQL configuration options, "extending" the base options
type MySQL struct {
	BaseDatabase
	TimeZone    string
	MaxOpenConn int
	MaxIdleConn int
	MaxLifeTime time.Duration
	MaxIdleTime time.Duration
}

//-----------------------------------------------------------------------------

// Factory
type MySQLFactory struct{}

//-----------------------------------------------------------------------------

// Convenience function, if we want to create a MySQL instance directly
func NewMySQL(mysql MySQL) (Database, error) {
	return NewDatabase("mysql", mysql)
}

//-----------------------------------------------------------------------------

func (f MySQLFactory) Create(database interface{}) (Database, error) {
	var mysql MySQL
	var ok bool

	if mysql, ok = database.(MySQL); !ok {
		return nil, fmt.Errorf("invalid type for database factory, expected database struct")
	}

	// Set defaults if needed
	if mysql.TimeZone == "" {
		mysql.TimeZone = "America/New_York"
	}
	if mysql.MaxOpenConn == 0 {
		mysql.MaxOpenConn = 1000
	}
	if mysql.MaxIdleConn == 0 {
		mysql.MaxIdleConn = 10
	}
	if mysql.MaxLifeTime == 0 {
		mysql.MaxLifeTime = 60
	}
	if mysql.MaxIdleTime == 0 {
		mysql.MaxIdleTime = 60
	}

	return mysql, nil
}

//-----------------------------------------------------------------------------

// GetConnection returns a *gorm.DB persistent for tenant
func (d MySQL) GetConnection() (*gorm.DB, error) {
	// Set the logger if injected
	var gormConfig *gorm.Config = nil
	if d.Logger != nil {
		gormConfig = &gorm.Config{
			Logger: d.Logger,
		}
	}

	conn, err := gorm.Open(mysql.Open(d.Dsn), gormConfig)
	if err != nil {
		return nil, err
	}
	// Set the database cache (gormcache) if injected
	if d.Cache != nil {
		conn.Use(d.Cache)
	}

	// Configure the connection pool
	sqlDB, err := conn.DB()
	if err != nil {
		return nil, err
	}

	// Set timezone
	tzSQL := fmt.Sprintf("SET time_zone='%s';", d.TimeZone)
	conn.Exec(tzSQL)
	if d.Debug {
		var sessionTimeZone string
		conn.Raw("SELECT @@session.time_zone").Scan(&sessionTimeZone)
		log.Println("Database timezone:", sessionTimeZone)
	}

	sqlDB.SetMaxOpenConns(d.MaxOpenConn)
	sqlDB.SetMaxIdleConns(d.MaxIdleConn)
	sqlDB.SetConnMaxLifetime(d.MaxLifeTime * time.Minute)
	sqlDB.SetConnMaxIdleTime(d.MaxIdleTime * time.Minute)

	return conn, nil
}

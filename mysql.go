package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

//-----------------------------------------------------------------------------

// Specific MySQL configuration options, "extending" the base options
type ConfigMySQL struct {
	BaseDatabase
	TimeZone    string
	MaxOpenConn int
	MaxIdleConn int
	MaxLifeTime time.Duration
	MaxIdleTime time.Duration
}

//-----------------------------------------------------------------------------

type MySQL struct{
	Config ConfigMySQL
}

//-----------------------------------------------------------------------------

// Factory
type MySQLFactory struct{}

//-----------------------------------------------------------------------------

// Convenience function, if we want to create a MySQL instance directly
func NewMySQL(mysql ConfigMySQL) (Database, error) {
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
	if mysql.Config.TimeZone == "" {
		mysql.Config.TimeZone = "America/New_York"
	}
	if mysql.Config.MaxOpenConn == 0 {
		mysql.Config.MaxOpenConn = 1000
	}
	if mysql.Config.MaxIdleConn == 0 {
		mysql.Config.MaxIdleConn = 10
	}
	if mysql.Config.MaxLifeTime == 0 {
		mysql.Config.MaxLifeTime = 60
	}
	if mysql.Config.MaxIdleTime == 0 {
		mysql.Config.MaxIdleTime = 60
	}

	return mysql, nil
}

//-----------------------------------------------------------------------------

// GetConnection returns a *gorm.DB persistent for tenant
func (d MySQL) GetConnection() (*gorm.DB, error) {
	// Get the cache connection key from the DSN
	hasher := sha1.New()
	hasher.Write([]byte(d.Config.Dsn))
	hash := hasher.Sum(nil)
	key := hex.EncodeToString(hash)

	// Return the cached connection
	if gdb, ok := DatabaseCache.Load(key); ok {
		return gdb.(*gorm.DB), nil
	}

	// Set the logger if injected
	var gormConfig *gorm.Config = nil
	if d.Config.Logger != nil {
		gormConfig = &gorm.Config{
			Logger: d.Config.Logger,
		}
	}

	conn, err := gorm.Open(mysql.Open(d.Config.Dsn), gormConfig)
	if err != nil {
		return nil, err
	}
	// Set the database cache (gormcache) if injected
	if d.Config.Cache != nil {
		conn.Use(d.Config.Cache)
	}

	// Configure the connection pool
	sqlDB, err := conn.DB()
	if err != nil {
		return nil, err
	}

	// Set timezone
	tzSQL := fmt.Sprintf("SET time_zone='%s';", d.Config.TimeZone)
	conn.Exec(tzSQL)
	if d.Config.Debug {
		var sessionTimeZone string
		conn.Raw("SELECT @@session.time_zone").Scan(&sessionTimeZone)
		log.Println("Database timezone:", sessionTimeZone)
	}

	sqlDB.SetMaxOpenConns(d.Config.MaxOpenConn)
	sqlDB.SetMaxIdleConns(d.Config.MaxIdleConn)
	sqlDB.SetConnMaxLifetime(d.Config.MaxLifeTime * time.Minute)
	sqlDB.SetConnMaxIdleTime(d.Config.MaxIdleTime * time.Minute)

	DatabaseCache.Store(key, conn)

	return conn, nil
}

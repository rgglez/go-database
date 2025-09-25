package database

import (
	"fmt"
	"sync"

	gormcache "github.com/rgglez/gormcache"
	gormzerolog "github.com/vitaliy-art/gorm-zerolog"
	"gorm.io/gorm"
)

//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------

// Database interface
type Database interface {
	GetConnection() (*gorm.DB, error)
}

//-----------------------------------------------------------------------------

// Database factory interface
type DatabaseFactory interface {
	Create(database interface{}) (Database, error)
}

//-----------------------------------------------------------------------------

// Base configuration options for the databases
type BaseDatabase struct {
	Dsn    string
	Logger *gormzerolog.GormLogger
	Cache  *gormcache.GormCache
	Debug  bool
}

//-----------------------------------------------------------------------------

// Connections cache
var DatabaseCache *sync.Map

//-----------------------------------------------------------------------------

// Factory registry
var factories = map[string]DatabaseFactory{
	"mysql": MySQLFactory{},
}

//-----------------------------------------------------------------------------

func NewDatabase(dbType string, database ...interface{}) (Database, error) {
	var factory DatabaseFactory
	var exists bool

	if factory, exists = factories[dbType]; !exists {
		return nil, fmt.Errorf("unknown database type: %s", dbType)
	}

	return factory.Create(database)
}

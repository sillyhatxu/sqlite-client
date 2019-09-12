package client

import "time"

type Config struct {
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	attempts        uint
	delay           time.Duration
	ddlPath         string
	flyway          bool
}

type Option func(*Config)

func MaxIdleConns(maxIdleConns int) Option {
	return func(c *Config) {
		c.maxIdleConns = maxIdleConns
	}
}

func MaxOpenConns(maxOpenConns int) Option {
	return func(c *Config) {
		c.maxOpenConns = maxOpenConns
	}
}

func ConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(c *Config) {
		c.connMaxLifetime = connMaxLifetime
	}
}

func Attempts(attempts uint) Option {
	return func(c *Config) {
		c.attempts = attempts
	}
}

func Delay(delay time.Duration) Option {
	return func(c *Config) {
		c.delay = delay
	}
}

func DDLPath(ddlPath string) Option {
	return func(c *Config) {
		c.ddlPath = ddlPath
		if ddlPath != "" {
			c.flyway = true
		}
	}
}

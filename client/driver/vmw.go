package driver

import (
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/nomad/structs"
	"time"
)

type VMWDriver struct {
	DriverContext
}

func NewVMWDriver(ctx *DriverContext) Driver {
	return &VMWDriver{DriverContext: *ctx}
}

func (c *VMWDriver) Validate(map[string]interface{}) error {
	return nil
}

func (d *VMWDriver) Abilities() DriverAbilities {
	return DriverAbilities{
		SendSignals: true,
	}
}

func (d *VMWDriver) Fingerprint(cfg *config.Config, node *structs.Node) (bool, error) {
	return true, nil
}

func (d *VMWDriver) Start(ctx *ExecContext, task *structs.Task) (DriverHandle, error) {
	return nil, nil
}

func (d *VMWDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	return nil, nil
}

func (d *VMWDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

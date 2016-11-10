package driver

import (
	"testing"

	ctestutils "github.com/hashicorp/nomad/client/testutil"
	"github.com/hashicorp/nomad/nomad/structs"
)

func TestImplements_Driver_Interface(t *testing.T) {
	//var _ VMWDriver = (*Driver)(nil)

	ctestutils.VMWCompatible(t)
	task := &structs.Task{
		Name:      "foo",
		Resources: structs.DefaultResources(),
	}
	_, execCtx := testDriverContexts(task)
	defer execCtx.AllocDir.Destroy()
	//	d := NewVMWDriver(driverCtx)

}

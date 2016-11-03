package driver

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/nomad/structs"

	ctestutils "github.com/hashicorp/nomad/client/testutil"
)

func TestImplements_Driver_Interface(t *testing.T) {
	//var _ VMWDriver = (*Driver)(nil)

	ctestutils.VMWCompatible(t)
	task := &structs.Task{
		Name:      "foo",
		Resources: structs.DefaultResources(),
	}
	driverCtx, execCtx := testDriverContexts(task)
	defer execCtx.AllocDir.Destroy()
	d := NewVMWDriver(driverCtx)

}

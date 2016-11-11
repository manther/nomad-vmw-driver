package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/allocdir"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/client/driver/executor"
	dstructs "github.com/hashicorp/nomad/client/driver/structs"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/helper/discover"
	"github.com/hashicorp/nomad/helper/fields"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"time"
)

const (
	// The key populated in Node Attributes to indicate the presence of the VMW
	// driver
	vmwDriverAttr = "driver.vmw"
)

type VMWDriver struct {
	memory        int
	cpus          int
	on            bool
	force         bool
	template      bool
	customization string
	waitForIP     bool

	Client         *govmomi.Client
	Datacenter     *object.Datacenter
	Datastore      *object.Datastore
	StoragePod     *object.StoragePod
	ResourcePool   *object.ResourcePool
	HostSystem     *object.HostSystem
	Folder         *object.Folder
	VirtualMachine *object.VirtualMachine
	Network        object.NetworkReference
	Device         types.BaseVirtualDevice
	Finder         *find.Finder

	DriverContext
}

// vmwHandle is returned from Start/Open as a handle to the PID
type vmwHandle struct {
	pluginClient   *plugin.Client
	vmName         string
	executor       executor.Executor
	allocDir       *allocdir.AllocDir
	killTimeout    time.Duration
	maxKillTimeout time.Duration
	logger         *log.Logger
	version        string
	waitCh         chan *dstructs.WaitResult
	doneCh         chan struct{}
	gctx           context.Context
	task           *object.Task
}

type VMWDriverConfig struct {
	Name           string `mapstructure:"name"`
	URL            string `mapstructure:"url"`
	DatacenterName string `mapstructure:"datacenter"`
	VMName         string `mapstructure:"vmname"`
	Network        string `mapstructure:"network"`
	Insecure       string `mapstructure:"insecure"`
	DatastoreName  string `mapstructure:"datastore"`
	Pool           string `mapstructure:"pool"`
	NetAdapterType string `mapstructure:"netadapter"`
}

type vmwId struct {
	VMName         string
	PluginConfig   *PluginReattachConfig
	AllocDir       *allocdir.AllocDir
	KillTimeout    time.Duration
	MaxKillTimeout time.Duration
}

func NewVMWDriver(ctx *DriverContext) Driver {
	return &VMWDriver{DriverContext: *ctx}
}

func NewVMWDriverConfig(task *structs.Task) (*VMWDriverConfig, error) {
	var driverConfig VMWDriverConfig
	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}
	return &driverConfig, nil
}

func (c *VMWDriver) Validate(config map[string]interface{}) error {
	fd := &fields.FieldData{
		Raw: config,
		Schema: map[string]*fields.FieldSchema{
			"name": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"url": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"datacenter": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"vmname": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"network": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"insecure": &fields.FieldSchema{
				Type: fields.TypeBool,
			},
			"datastore": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"pool": &fields.FieldSchema{
				Type: fields.TypeString,
			},
			"netadapter": &fields.FieldSchema{
				Type: fields.TypeString,
			},
		},
	}

	if err := fd.Validate(); err != nil {
		return err
	}
	return nil
}

func (d *VMWDriver) Abilities() DriverAbilities {
	return DriverAbilities{
		SendSignals: true,
	}
}

func (d *VMWDriver) Fingerprint(cfg *config.Config, node *structs.Node) (bool, error) {
	// Get the current status so that we can log any debug messages only if the
	// state changes
	_, currentlyEnabled := node.Attributes[vmwDriverAttr]
	if !currentlyEnabled {
		d.logger.Printf("[DEBUG] driver.vmw: enabling driver")
	}
	node.Attributes[vmwDriverAttr] = "1"
	return true, nil
}

func (d *VMWDriver) Start(ctx *ExecContext, task *structs.Task) (DriverHandle, error) {
	_, err := d.CloneVM(ctx, task)
	if err != nil {
		d.logger.Printf("[ERR] Error deploying VM. Task: %s. Error: %s", task.Name, err)
		return nil, err
	} else {
		d.logger.Printf("[INFO] Task: %s VM deployed successfully!", task.Name)
	}
	return nil, nil
}

func (d *VMWDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	return nil, nil
}

func (d *VMWDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

func (d *VMWDriver) CloneVM(ctx *ExecContext, task *structs.Task) (DriverHandle, error) {
	//d.logger.Printf("Here1")
	vmwDriverConfig, err := NewVMWDriverConfig(task)
	if err != nil {
		return nil, err
	}

	// ESXi of VSphere Host URL
	// TODO take URL and user, pass separately
	// for generating unique VM names with the username later
	hosturl, err := url.Parse(vmwDriverConfig.URL)

	gctx := context.TODO()
	// Instantiate new govmomi client
	d.Client, err = govmomi.NewClient(gctx, hosturl, true)
	if err != nil {
		return nil, err
	}

	// Grab datacenter object
	dc, err := getDatacenter(d.Client, vmwDriverConfig.DatacenterName, gctx)
	if err != nil {
		return nil, err
	}

	d.Finder = find.NewFinder(d.Client.Client, true)
	d.Finder = d.Finder.SetDatacenter(dc)

	// Find VM to clone
	d.VirtualMachine, err = d.Finder.VirtualMachine(gctx, vmwDriverConfig.VMName)
	if err != nil {
		return nil, err
	}

	// Search for the first network card of the source
	devices, err := d.VirtualMachine.Device(gctx)
	if err != nil {
		return nil, err
	}

	var card *types.VirtualEthernetCard
	for _, device := range devices {
		if c, ok := device.(types.BaseVirtualEthernetCard); ok {
			card = c.GetVirtualEthernetCard()
			break
		}
	}
	if card == nil {
		return nil, fmt.Errorf("No network device found.")
	}
	//d.logger.Printf("Here2")
	if d.Network, err = d.Finder.NetworkOrDefault(gctx, vmwDriverConfig.Network); err != nil {
		return nil, err
	}

	backing, err := d.Network.EthernetCardBackingInfo(gctx)
	if err != nil {
		return nil, err
	}

	d.Device, err = object.EthernetCardTypes().CreateEthernetCard(vmwDriverConfig.NetAdapterType, backing)
	if err != nil {
		return nil, err
	}

	// Set backing info
	card.Backing = d.Device.(types.BaseVirtualEthernetCard).GetVirtualEthernetCard().Backing

	// Prepare virtual device config spec for network card
	configSpecs := []types.BaseVirtualDeviceConfigSpec{
		&types.VirtualDeviceConfigSpec{
			Operation: types.VirtualDeviceConfigSpecOperationEdit,
			Device:    card,
		},
	}

	// Get resource pool object
	d.Folder, err = Folder(d.Finder, "", gctx)
	if err != nil {
		return nil, err
	}
	folderref := d.Folder.Reference()
	d.ResourcePool, err = ResourcePool(d.Finder, vmwDriverConfig.Pool, gctx)
	if err != nil {
		return nil, err
	}
	poolref := d.ResourcePool.Reference()

	// Not doing relocates yet, but leaving in for later
	relocateSpec := types.VirtualMachineRelocateSpec{
		DeviceChange: configSpecs,
		Folder:       &folderref,
		Pool:         &poolref,
	}

	// TODO
	//cmd.HostSystem, err = HostSystem(cmd.Finder)
	//if err != nil {
	//	return nil, err
	//}
	if d.HostSystem != nil {
		hostref := d.HostSystem.Reference()
		relocateSpec.Host = &hostref
	}

	// Pulling clone specifications
	cloneSpec := &types.VirtualMachineCloneSpec{
		Location: relocateSpec,
		PowerOn:  false,
		Template: d.template,
	}

	// Get datastore
	d.Datastore, err = Datastore(d.Finder, vmwDriverConfig.DatastoreName, gctx)
	if err != nil {
		return nil, err
	}

	// Clone to storage pod
	datastoreref := types.ManagedObjectReference{}
	if d.StoragePod != nil && d.Datastore == nil {
		storagePod := d.StoragePod.Reference()

		// Build pod selection spec from config spec
		podSelectionSpec := types.StorageDrsPodSelectionSpec{
			StoragePod: &storagePod,
		}

		// Get the virtual machine reference
		vmref := d.VirtualMachine.Reference()

		// Build the placement spec
		storagePlacementSpec := types.StoragePlacementSpec{
			Folder:           &folderref,
			Vm:               &vmref,
			CloneName:        vmwDriverConfig.Name,
			CloneSpec:        cloneSpec,
			PodSelectionSpec: podSelectionSpec,
			Type:             string(types.StoragePlacementSpecPlacementTypeClone),
		}

		// Get the storage placement result
		storageResourceManager := object.NewStorageResourceManager(d.Client.Client)
		result, err := storageResourceManager.RecommendDatastores(gctx, storagePlacementSpec)
		if err != nil {
			return nil, err
		}

		// Get the recommendations
		recommendations := result.Recommendations
		if len(recommendations) == 0 {
			return nil, fmt.Errorf("no recommendations")
		}

		// Get the first recommendation
		datastoreref = recommendations[0].Action[0].(*types.StoragePlacementAction).Destination
	} else if d.StoragePod == nil && d.Datastore != nil {
		datastoreref = d.Datastore.Reference()
	} else {
		return nil, fmt.Errorf("Please provide either a datastore or a storagepod")
	}

	// Set the destination datastore
	cloneSpec.Location.Datastore = &datastoreref

	// Check if vmx already exists
	if !d.force {
		vmxPath := fmt.Sprintf("%s/%s.vmx", vmwDriverConfig.Name, vmwDriverConfig.Name)

		var mds mo.Datastore
		err = property.DefaultCollector(d.Client.Client).RetrieveOne(gctx, datastoreref, []string{"name"}, &mds)
		if err != nil {
			return nil, err
		}

		datastore := object.NewDatastore(d.Client.Client, datastoreref)
		datastore.InventoryPath = mds.Name

		_, err := datastore.Stat(gctx, vmxPath)
		if err == nil {
			dsPath := d.Datastore.Path(vmxPath)
			return nil, fmt.Errorf("File %s already exists", dsPath)
		}
	}
	// check if customization specification requested
	if len(d.customization) > 0 {
		// get the customization spec manager
		customizationSpecManager := object.NewCustomizationSpecManager(d.Client.Client)
		// check if customization specification exists
		exists, err := customizationSpecManager.DoesCustomizationSpecExist(gctx, d.customization)
		if err != nil {
			return nil, err
		}
		if exists == false {
			return nil, fmt.Errorf("Customization specification %s does not exists.", d.customization)
		}
		// get the customization specification
		customSpecItem, err := customizationSpecManager.GetCustomizationSpec(gctx, d.customization)
		if err != nil {
			return nil, err
		}
		customSpec := customSpecItem.Spec
		// set the customization
		cloneSpec.Customization = &customSpec
	}

	// Setup Nomad executable
	taskDir, ok := ctx.AllocDir.TaskDirs[d.DriverContext.taskName]
	if !ok {
		return nil, fmt.Errorf("Could not find task directory for task: %v", d.DriverContext.taskName)
	}

	bin, err := discover.NomadExecutable()
	if err != nil {
		return nil, fmt.Errorf("unable to find the nomad binary: %v", err)
	}
	pluginLogFile := filepath.Join(taskDir, fmt.Sprintf("%s-executor.out", task.Name))
	pluginConfig := &plugin.ClientConfig{
		Cmd: exec.Command(bin, "executor", pluginLogFile),
	}

	executorPlugin, pluginClient, err := createExecutor(pluginConfig, d.config.LogOutput, d.config)
	if err != nil {
		return nil, err
	}
	executorCtx := &executor.ExecutorContext{
		TaskEnv:  d.taskEnv,
		Task:     task,
		Driver:   "vmw",
		AllocDir: ctx.AllocDir,
		AllocID:  ctx.AllocID,
	}
	if err := executorPlugin.SetContext(executorCtx); err != nil {
		pluginClient.Kill()
		return nil, fmt.Errorf("failed to set executor context: %v", err)
	}

	// Clone virtual machine
	result, err := d.VirtualMachine.Clone(gctx, d.Folder, vmwDriverConfig.Name, *cloneSpec)
	if err != nil {
		pluginClient.Kill()
		return nil, err
	}

	// Return a driver handle
	maxKill := d.DriverContext.config.MaxKillTimeout
	h := &vmwHandle{
		pluginClient:   pluginClient,
		executor:       executorPlugin,
		allocDir:       ctx.AllocDir,
		killTimeout:    GetKillTimeout(task.KillTimeout, maxKill),
		maxKillTimeout: maxKill,
		logger:         d.logger,
		version:        d.config.Version,
		doneCh:         make(chan struct{}),
		waitCh:         make(chan *dstructs.WaitResult, 1),
		gctx:           gctx,
		task:           result,
	}
	if err := executorPlugin.SyncServices(consulContext(d.config, "")); err != nil {
		d.logger.Printf("[ERR] driver.vmw: error registering services with consul for task: %q: %v", task.Name, err)
	}
	fmt.Printf("Result type %v", reflect.TypeOf(result))
	go h.run()
	return h, nil
}

func (h *vmwHandle) run() {
	fmt.Printf("Here0")
	// TODO define a wait process
	// Query the host to see that the VM is stood up and powered on.
	_, err := h.task.WaitForResult(h.gctx, nil)
	if err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed while waiting for host task to complete: %v", err)
	}

	fmt.Printf("Here1")
	close(h.doneCh)
	h.waitCh <- dstructs.NewWaitResult(0, 0, nil)
	close(h.waitCh)

	// Remove services
	fmt.Printf("Here2")
	if err := h.executor.DeregisterServices(); err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed to deregister services: %v", err)
	}
	fmt.Printf("Here3")
	if err := h.executor.Exit(); err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed to exit: %v", err)
	}
	fmt.Printf("Here4")
	h.pluginClient.Kill()
}

func (h *vmwHandle) ID() string {
	id := vmwId{
		VMName:         h.vmName,
		KillTimeout:    h.killTimeout,
		MaxKillTimeout: h.maxKillTimeout,
		PluginConfig:   NewPluginReattachConfig(h.pluginClient.ReattachConfig()),
		AllocDir:       h.allocDir,
	}

	data, err := json.Marshal(id)
	if err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed to marshal ID to JSON: %s", err)
	}
	return string(data)
}

func (h *vmwHandle) WaitCh() chan *dstructs.WaitResult {
	return h.waitCh
}

func (h *vmwHandle) Update(task *structs.Task) error {
	// Store the updated kill timeout.
	h.killTimeout = GetKillTimeout(task.KillTimeout, h.maxKillTimeout)
	h.executor.UpdateTask(task)

	// Update is not possible
	return nil
}

func (h *vmwHandle) Signal(s os.Signal) error {
	return fmt.Errorf("VMW driver can't send signals")
}

func (h *vmwHandle) Kill() error {
	if err := h.executor.ShutDown(); err != nil {
		if h.pluginClient.Exited() {
			return nil
		}
		return fmt.Errorf("executor Shutdown failed: %v", err)
	}

	select {
	case <-h.doneCh:
		return nil
	case <-time.After(h.killTimeout):
		if h.pluginClient.Exited() {
			return nil
		}
		if err := h.executor.Exit(); err != nil {
			return fmt.Errorf("executor Exit failed: %v", err)
		}

		return nil
	}
}

func (h *vmwHandle) Stats() (*cstructs.TaskResourceUsage, error) {
	return h.executor.Stats()
}

func getDatacenter(c *govmomi.Client, dc string, gctx context.Context) (*object.Datacenter, error) {
	finder := find.NewFinder(c.Client, true)
	if dc != "" {
		d, err := finder.Datacenter(gctx, dc)
		return d, err
	} else {
		d, err := finder.DefaultDatacenter(gctx)
		return d, err
	}
}

func Folder(finder *find.Finder, name string, gctx context.Context) (*object.Folder, error) {

	if folder, err := finder.FolderOrDefault(gctx, name); err != nil {
		return nil, err
	} else {
		return folder, nil
	}
}

func ResourcePool(f *find.Finder, name string, gctx context.Context) (*object.ResourcePool, error) {
	if pool, err := f.ResourcePoolOrDefault(gctx, name); err != nil {
		return nil, err
	} else {
		return pool, nil
	}
}

func Datastore(f *find.Finder, name string, gctx context.Context) (*object.Datastore, error) {
	if ds, err := f.DatastoreOrDefault(gctx, name); err != nil {
		return nil, err
	} else {
		return ds, nil
	}
}

//func HostSystem(f *find.Finder) (*object.HostSystem, error) {
//	host, err := f.DefaultHostSystem(context)
//	return host, err
//}

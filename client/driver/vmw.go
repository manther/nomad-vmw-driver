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
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"
	//"github.com/vmware/govmomi/govc/vm"
	//"github.com/vmware/govmomi/task"
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
	logger         log.Logger
	version        string
	waitCh         chan *dstructs.WaitResult
	doneCh         chan struct{}
	task           object.Task
	client         *vim25.Client
	vm             *object.VirtualMachine
	resourceUsage  *cstructs.TaskResourceUsage
}

type VMWDriverConfig struct {
	Name           string `mapstructure:"name"`
	URL            string `mapstructure:"url"`
	DatacenterName string `mapstructure:"datacenter"`
	VMPath         string `mapstructure:"vmpath"`
	Network        string `mapstructure:"network"`
	Insecure       string `mapstructure:"insecure"`
	DatastoreName  string `mapstructure:"datastore"`
	Pool           string `mapstructure:"pool"`
	NetAdapterType string `mapstructure:"netadapter"`
	PowerOn        bool   `mapstructure:"poweron"`
	Username       string `mapstructure:"username"`
	Password       string `mapstructure:"password"`
	GenUniqueName  bool   `mapstructure:"genuniquename"`
	Folder         string `mapstructure:"folder"`
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
				Type:     fields.TypeString,
				Required: true,
			},
			"datacenter": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"vmpath": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"network": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"insecure": &fields.FieldSchema{
				Type:     fields.TypeBool,
				Required: true,
			},
			"datastore": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"pool": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"netadapter": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: true,
			},
			"poweron": &fields.FieldSchema{
				Type:     fields.TypeBool,
				Required: false,
			},
			"username": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: false,
			},
			"password": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: false,
			},
			"genuniquename": &fields.FieldSchema{
				Type:     fields.TypeBool,
				Required: false,
			},
			"folder": &fields.FieldSchema{
				Type:     fields.TypeString,
				Required: false,
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
	// TODO look at env vars for API url port
	handle, err := d.CloneVM(ctx, task)
	if err != nil {
		d.logger.Printf("[ERR] driver.vmw: Error issuing VM clone command. Task: %s. Error: %v", task.Name, err)
		return handle, err
	} else {
		d.logger.Printf("[INFO] driver.vmw: VM clone command issued successfully. Task name : %s", task.Name)
	}

	return handle, nil
}

func (d *VMWDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	return nil, nil
}

func (d *VMWDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

func (d *VMWDriver) CloneVM(ctx *ExecContext, task *structs.Task) (DriverHandle, error) {
	// TODO take param to power on VM, default to true
	// TODO possibly break this function up into modular reusable pieces
	vmwDriverConfig, err := NewVMWDriverConfig(task)
	if err != nil {
		return nil, err
	}

	// ESXi of VSphere Host URL
	// TODO take URL and user, pass separately
	// for generating unique VM names with the username later
	hosturl, err := url.Parse(vmwDriverConfig.URL)

	gctx := context.Background()
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
	d.VirtualMachine, err = d.Finder.VirtualMachine(gctx, vmwDriverConfig.VMPath)
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

	// Get Folder
	d.Folder, err = Folder(d.Finder, vmwDriverConfig.Folder, gctx)
	if err != nil {
		return nil, err
	}
	folderref := d.Folder.Reference()

	// Get resource pool object
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

	// Setting clone specifications
	cloneSpec := &types.VirtualMachineCloneSpec{
		Location: relocateSpec,
		PowerOn:  false,
		Template: d.template,
		Config: &types.VirtualMachineConfigSpec{
			CpuAllocation:    d.GetResourceAllocationInfo(int32(task.Resources.CPU)),
			MemoryAllocation: d.GetResourceAllocationInfo(int32(task.Resources.MemoryMB)),
		},
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

	if vmwDriverConfig.GenUniqueName {
		// TODO Gen unique name possibly using 	vmwDriverConfig.Name + username + uid
	}

	// Clone virtual machine
	d.logger.Printf("[INFO] driver.vmw: clone spec %v", cloneSpec)
	vmTask, err := d.VirtualMachine.Clone(gctx, d.Folder, vmwDriverConfig.Name, *cloneSpec)
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
		logger:         *d.logger,
		version:        d.config.Version,
		doneCh:         make(chan struct{}),
		waitCh:         make(chan *dstructs.WaitResult, 1),
		task:           *vmTask,
		client:         d.Client.Client,
	}
	if err := executorPlugin.SyncServices(consulContext(d.config, "")); err != nil {
		d.logger.Printf("[ERR] driver.vmw: error registering services with consul for task: %q: %v", task.Name, err)
	}

	go h.run()

	return h, nil
}

func (s *VMWDriver) GetResourceAllocationInfo(shares int32) *types.ResourceAllocationInfo {
	return &types.ResourceAllocationInfo{
		ExpandableReservation: types.NewBool(true),
		Shares: &types.SharesInfo{
			Level:  types.SharesLevelLow,
			Shares: shares,
		},
	}
}

func (h *vmwHandle) run() {
	h.logger.Printf("[INFO] driver.vmw: Waiting for clone confirmation.")
	vmInfo, err := h.task.WaitForResult(context.TODO(), nil)
	if err != nil {
		h.logger.Printf("[ERR] driver.vmw: Error issuing cloning VM. Error: %v", err)
	} else {
		h.vm = object.NewVirtualMachine(h.client, vmInfo.Result.(types.ManagedObjectReference))
		h.logger.Printf("[INFO] driver.vmw: VM cloned.")
		h.logger.Printf("[INFO] driver.vmw: Powering on VM, and waiting for result.")
		taskPowerOn, err := h.vm.PowerOn(context.TODO())
		if err != nil {
			h.logger.Printf("[ERR] driver.vmw: Error issuing powering on command for VM. Error: %v", err)
		}

		powerInfo, err := taskPowerOn.WaitForResult(context.TODO(), nil)
		if err != nil {
			h.logger.Printf("[ERR] driver.vmw: Error powering on VM. Error: %v", err)
		}
		h.logger.Printf("[INFO] driver.vmw: VM powered on. Power on time: %v. Waiting for IP", powerInfo.StartTime)

		_, err = h.vm.WaitForIP(context.TODO())
		if err != nil {
			h.logger.Printf("[ERR] driver.vmw: Error waiting for IP address for VM. Error: %v", err)
		}
		h.logger.Printf("[INFO] driver.vmw: VM: %v deployment successful!", h.vm.InventoryPath)
		// TODO set resulting UUID in handle
	}

	close(h.doneCh)
	h.waitCh <- dstructs.NewWaitResult(0, 0, nil)
	close(h.waitCh)

	// Remove services
	if err := h.executor.DeregisterServices(); err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed to deregister services: %v", err)
	}
	if err := h.executor.Exit(); err != nil {
		h.logger.Printf("[ERR] driver.vmw: failed to exit: %v", err)
	}
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
	// TODO vm.power down
	// TODO VM.destroy
	// TODO Use the UUI that comes back from run()
	return nil
}

type infoResult struct {
	VirtualMachines []mo.VirtualMachine
	objects         []*object.VirtualMachine
	entities        map[types.ManagedObjectReference]string
	//cmd             *info
}

func (h *vmwHandle) Stats() (*cstructs.TaskResourceUsage, error) {
	//var props = []string{"summary", "guest.ipAddress", "datastore", "network"}
	//pc := property.DefaultCollector(h.client.Client)
	//var vms = mo.VirtualMachine{}
	//var res infoResult
	//res.VirtualMachines = make([]mo.VirtualMachine, 1)
	//res.VirtualMachines[0] = vms
	//res.objects = make([]*object.VirtualMachine, 1)
	//res.objects[0] = h.vm
	//err := pc.RetrieveOne(context.TODO(), h.vm.Reference(), props, &res.VirtualMachines)
	//if err != nil {
	//	fmt.Printf("[ERR] driver.vmw: Error getting test result Error: %v", err)
	//} else {
	//	res.collectReferences(pc, context.TODO())
	//	//fmt.Printf("[INFO] driver.vmw: Test result")
	//	//fmt.Printf("Map len: %v", len(res.entities))
	//	for k, v := range res.entities {
	//		//fmt.Printf("in")
	//		fmt.Printf("key[%v] value[%v]\n", k, v)
	//	}
	//	objects := make(map[types.ManagedObjectReference]mo.VirtualMachine, len(res.VirtualMachines))
	//	for _, o := range res.VirtualMachines {
	//		objects[o.Reference()] = o
	//	}
	//	for _, o := range res.objects {
	//		vm := objects[o.Reference()]
	//		s := vm.Summary
	//
	//		fmt.Printf("Name: %s\n", s.Config.Name)
	//		//ms := &cstructs.MemoryStats{
	//		//	R
	//		//}
	//		//cs := &cstructs.CpuStats{}
	//	}
	//}
	return h.resourceUsage, nil
}

func getDatacenter(c *govmomi.Client, dc string, ctx context.Context) (*object.Datacenter, error) {
	finder := find.NewFinder(c.Client, true)
	if dc != "" {
		d, err := finder.Datacenter(ctx, dc)
		return d, err
	} else {
		d, err := finder.DefaultDatacenter(ctx)
		return d, err
	}
}

func Folder(finder *find.Finder, name string, ctx context.Context) (*object.Folder, error) {
	if folder, err := finder.FolderOrDefault(ctx, name); err != nil {
		return nil, err
	} else {
		return folder, nil
	}
}

func ResourcePool(f *find.Finder, name string, ctx context.Context) (*object.ResourcePool, error) {
	if pool, err := f.ResourcePoolOrDefault(ctx, name); err != nil {
		return nil, err
	} else {
		return pool, nil
	}
}

func Datastore(f *find.Finder, name string, ctx context.Context) (*object.Datastore, error) {
	if ds, err := f.DatastoreOrDefault(ctx, name); err != nil {
		return nil, err
	} else {
		return ds, nil
	}
}

//func HostSystem(f *find.Finder) (*object.HostSystem, error) {
//	host, err := f.DefaultHostSystem(context)
//	return host, err
//}

func (r *infoResult) collectReferences(pc *property.Collector, ctx context.Context) error {
	r.entities = make(map[types.ManagedObjectReference]string) // MOR -> Name map

	var host []mo.HostSystem
	var network []mo.Network
	var opaque []mo.OpaqueNetwork
	var dvp []mo.DistributedVirtualPortgroup
	var datastore []mo.Datastore
	// Table to drive inflating refs to their mo.* counterparts (dest)
	// and save() the Name to r.entities w/o using reflection here.
	// Note that we cannot use a []mo.ManagedEntity here, since mo.Network has its own 'Name' field,
	// the mo.Network.ManagedEntity.Name field will not be set.
	vrefs := map[string]*struct {
		dest interface{}
		refs []types.ManagedObjectReference
		save func()
	}{
		"HostSystem": {
			&host, nil, func() {
				for _, e := range host {
					r.entities[e.Reference()] = e.Name
				}
			},
		},
		"Network": {
			&network, nil, func() {
				for _, e := range network {
					r.entities[e.Reference()] = e.Name
				}
			},
		},
		"OpaqueNetwork": {
			&opaque, nil, func() {
				for _, e := range opaque {
					r.entities[e.Reference()] = e.Name
				}
			},
		},
		"DistributedVirtualPortgroup": {
			&dvp, nil, func() {
				for _, e := range dvp {
					r.entities[e.Reference()] = e.Name
				}
			},
		},
		"Datastore": {
			&datastore, nil, func() {
				for _, e := range datastore {
					r.entities[e.Reference()] = e.Name
				}
			},
		},
	}

	xrefs := make(map[types.ManagedObjectReference]bool)
	// Add MOR to vrefs[kind].refs avoiding any duplicates.
	addRef := func(refs ...types.ManagedObjectReference) {
		for _, ref := range refs {
			if _, exists := xrefs[ref]; exists {
				return
			}
			xrefs[ref] = true
			vref := vrefs[ref.Type]
			vref.refs = append(vref.refs, ref)
		}
	}

	for _, vm := range r.VirtualMachines {
		//if r.cmd.General {
		if ref := vm.Summary.Runtime.Host; ref != nil {
			addRef(*ref)
		}
		//}

		//if r.cmd.Resources {
		addRef(vm.Datastore...)
		addRef(vm.Network...)
		//}
	}

	for _, vref := range vrefs {
		if vref.refs == nil {
			continue
		}
		err := pc.Retrieve(ctx, vref.refs, []string{"name"}, vref.dest)
		if err != nil {
			return err
		}
		vref.save()
	}

	return nil
}

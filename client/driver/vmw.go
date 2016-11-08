package driver

import (
	"fmt"
	"context"
	"net/url"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/find"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/nomad/structs"
	"time"
)

type VMWDriver struct {
	name           string
	memory         int
	cpus           int
	on             bool
	force          bool
	template       bool
	customization  string
	waitForIP      bool

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
	vmw := &VMWDriver{name: "new-zombie-nomad-vm"}
	_, err := vmw.cloneVM(ctx)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	return nil, nil
}

func (d *VMWDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	return nil, nil
}

func (d *VMWDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

func deploy_vm(ctx context.Context) {
	vmw := &VMWDriver{name: "new-zombie-nomad-vm"}
	_, err := vmw.cloneVM(ctx)
	if err != nil {
		//fmt.Printf("%v\n", err)
	}
}

func (job *VMWDriver) cloneVM(ctx context.Context) (*object.Task, error) {
	url, err := url.Parse("https://root:vmware@10.144.32.100/sdk")

	job.Client, err = govmomi.NewClient(ctx, url, true)
	if err != nil {
		return nil, err
	}

	dc, err := getDatacenter(job.Client, "Lab")
	if err != nil {
		return nil, err
	}

	job.Finder = find.NewFinder(job.Client.Client, true)
	job.Finder = job.Finder.SetDatacenter(dc)
	job.VirtualMachine, err = job.Finder.VirtualMachine(ctx, "zombie-sandbox")
	if err != nil {
		return nil, err
	}

	// search for the first network card of the source
	devices, err := job.VirtualMachine.Device(ctx)
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

	if job.Network, err = job.Finder.NetworkOrDefault(context.TODO(), "WDC_EXTERNAL"); err != nil {
		return nil, err
	}

	backing, err := job.Network.EthernetCardBackingInfo(context.TODO())
	if err != nil {
		return nil, err
	}

	job.Device, err = object.EthernetCardTypes().CreateEthernetCard("e1000", backing)
	if err != nil {
		return nil, err
	}

	//set backing info
	card.Backing = job.Device.(types.BaseVirtualEthernetCard).GetVirtualEthernetCard().Backing

	// prepare virtual device config spec for network card
	configSpecs := []types.BaseVirtualDeviceConfigSpec{
		&types.VirtualDeviceConfigSpec{
			Operation: types.VirtualDeviceConfigSpecOperationEdit,
			Device:    card,
		},
	}

	job.Folder, err = Folder(job.Finder, "")
	if err != nil {
		return nil, err
	}
	folderref := job.Folder.Reference()
	job.ResourcePool, err = ResourcePool(job.Finder, "MGMT")
	if err != nil {
		return nil, err
	}
	poolref := job.ResourcePool.Reference()

	relocateSpec := types.VirtualMachineRelocateSpec{
		DeviceChange: configSpecs,
		Folder:       &folderref,
		Pool:         &poolref,
	}

	//cmd.HostSystem, err = HostSystem(cmd.Finder)
	//if err != nil {
	//	return nil, err
	//}
	if job.HostSystem != nil {
		hostref := job.HostSystem.Reference()
		relocateSpec.Host = &hostref
	}

	cloneSpec := &types.VirtualMachineCloneSpec{
		Location: relocateSpec,
		PowerOn:  false,
		Template: job.template,
	}

	// get datastore
	job.Datastore, err = Datastore(job.Finder, "automation_lab_pool_0")
	if err != nil {
		return nil, err
	}

	// clone to storage pod
	datastoreref := types.ManagedObjectReference{}
	if job.StoragePod != nil && job.Datastore == nil {
		storagePod := job.StoragePod.Reference()

		// Build pod selection spec from config spec
		podSelectionSpec := types.StorageDrsPodSelectionSpec{
			StoragePod: &storagePod,
		}

		// Get the virtual machine reference
		vmref := job.VirtualMachine.Reference()

		// Build the placement spec
		storagePlacementSpec := types.StoragePlacementSpec{
			Folder:           &folderref,
			Vm:               &vmref,
			CloneName:        job.name,
			CloneSpec:        cloneSpec,
			PodSelectionSpec: podSelectionSpec,
			Type:             string(types.StoragePlacementSpecPlacementTypeClone),
		}

		// Get the storage placement result
		storageResourceManager := object.NewStorageResourceManager(job.Client.Client)
		result, err := storageResourceManager.RecommendDatastores(ctx, storagePlacementSpec)
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
	} else if job.StoragePod == nil && job.Datastore != nil {
		datastoreref = job.Datastore.Reference()
	} else {
		return nil, fmt.Errorf("Please provide either a datastore or a storagepod")
	}

	// Set the destination datastore
	cloneSpec.Location.Datastore = &datastoreref

	// Check if vmx already exists
	if !job.force {
		vmxPath := fmt.Sprintf("%s/%s.vmx", job.name, job.name)

		var mds mo.Datastore
		err = property.DefaultCollector(job.Client.Client).RetrieveOne(ctx, datastoreref, []string{"name"}, &mds)
		if err != nil {
			return nil, err
		}

		datastore := object.NewDatastore(job.Client.Client, datastoreref)
		datastore.InventoryPath = mds.Name

		_, err := datastore.Stat(ctx, vmxPath)
		if err == nil {
			dsPath := job.Datastore.Path(vmxPath)
			return nil, fmt.Errorf("File %s already exists", dsPath)
		}
	}

	// check if customization specification requested
	if len(job.customization) > 0 {
		// get the customization spec manager
		customizationSpecManager := object.NewCustomizationSpecManager(job.Client.Client)
		// check if customization specification exists
		exists, err := customizationSpecManager.DoesCustomizationSpecExist(ctx, job.customization)
		if err != nil {
			return nil, err
		}
		if exists == false {
			return nil, fmt.Errorf("Customization specification %s does not exists.", job.customization)
		}
		// get the customization specification
		customSpecItem, err := customizationSpecManager.GetCustomizationSpec(ctx, job.customization)
		if err != nil {
			return nil, err
		}
		customSpec := customSpecItem.Spec
		// set the customization
		cloneSpec.Customization = &customSpec
	}

	// clone virtualmachine
	return job.VirtualMachine.Clone(ctx, job.Folder, job.name, *cloneSpec)
}

// getDatacenter gets datacenter object
func getDatacenter(c *govmomi.Client, dc string) (*object.Datacenter, error) {
	finder := find.NewFinder(c.Client, true)
	if dc != "" {
		d, err := finder.Datacenter(context.TODO(), dc)
		return d, err
	} else {
		d, err := finder.DefaultDatacenter(context.TODO())
		return d, err
	}
}

func Folder(finder *find.Finder, name string) (*object.Folder, error) {

	if folder, err := finder.FolderOrDefault(context.TODO(), name); err != nil {
		return nil, err
	} else {
		return folder, nil
	}
}

func ResourcePool(f *find.Finder, name string) (*object.ResourcePool, error) {
	if pool, err := f.ResourcePoolOrDefault(context.TODO(), name); err != nil {
		return nil, err
	} else {
		return pool, nil
	}
}

func HostSystem(f *find.Finder) (*object.HostSystem, error) {
	host, err := f.DefaultHostSystem(context.TODO())
	return host, err
}

func Datastore(f *find.Finder, name string) (*object.Datastore, error) {
	if ds, err := f.DatastoreOrDefault(context.TODO(), name); err != nil {
		return nil, err
	} else {
		return ds, nil
	}
}


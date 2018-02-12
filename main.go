package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/op/go-logging"
	"golang.org/x/sync/syncmap"
	"gopkg.in/lxc/go-lxc.v2"
)

var (
	log        = logging.MustGetLogger("director/track")
	errorcount = 0
)

func main() {
	d := &trackDirector{
		Template: "stresstest",
	}

	d.activeContainers = &syncmap.Map{}

	id := 1
	r := rand.New(rand.NewSource(99))

	for {
		log.Info("Creating container %s", id)
		go d.Create(id)
		// Second connection wanting the same container
		if r.Int() > 40 {
			time.Sleep(50 * time.Millisecond)
			go d.Create(id)
		}
		// Third connection wanting the same container
		if r.Int() > 60 {
			time.Sleep(50 * time.Millisecond)
			go d.Create(id)
		}
		// Fourth connection wanting the same container
		if r.Int() > 80 {
			time.Sleep(50 * time.Millisecond)
			go d.Create(id)
		}
		time.Sleep(600 * time.Millisecond)
		id++
	}
}

func (d *trackDirector) Create(id int) error {
	meta, err := d.getContainerMeta(id)
	if err != nil {
		log.Errorf("Error getting container meta %s: %s", meta.name, err.Error())
		errorcount++
		return err
	}

	meta.m.Lock()
	defer meta.m.Unlock()
	if !meta.c.Running() {
		err := meta.start()
		if err != nil {
			log.Errorf("Error starting container %s: %s", meta.name, err.Error())
			errorcount++
			return err
		}
	}

	if meta.idevice == "" {
		err = meta.getNetwork()
		if err != nil {
			log.Errorf("Error getting network for container %s: %s", meta.name, err.Error())
			errorcount++
			return err
		}
	}

	log.Infof("Started container %s", id)
	return nil
}

func (d *trackDirector) getContainerMeta(id int) (*containerMeta, error) {

	name := fmt.Sprintf("stress-%d", id)
	// Get meta from cache, or store a clean one
	m, found := d.activeContainers.LoadOrStore(name, &containerMeta{
		d:        d,
		name:     name,
		idle:     time.Time{},
		template: d.Template,
	})

	meta := m.(*containerMeta)

	if !found {
		// Container not in cache, check state
		meta.m.Lock()
		defer meta.m.Unlock()
		handle, exists := d.checkExists(name)
		if !exists {
			// Container doesn't exist yet, create a new one from template
			cloneHandle, err := d.cloneWithName(name)
			if err != nil {
				return nil, err
			}
			meta.c = cloneHandle
		} else {
			meta.c = handle
		}
	}
	return meta, nil
}

func (d *trackDirector) cloneWithName(name string) (*lxc.Container, error) {
	log.Debugf("Cloning new container %s from template %s", name, d.Template)
	templateHandle, err := lxc.NewContainer(d.Template)
	if err != nil {
		log.Errorf("Error getting handle on template for %s: %s", name, err.Error())
		return nil, err
	}

	defer lxc.Release(templateHandle)

	// Attempt to clone the meta
	if err = templateHandle.Clone(name, lxc.CloneOptions{
		Backend:  lxc.Overlayfs,
		Snapshot: true,
		KeepName: true,
	}); err != nil {
		log.Errorf("Error cloning %s: %s", name, err.Error())
		return nil, err
	}

	newContainerHandle, err := lxc.NewContainer(name)
	if err != nil {
		log.Errorf("Error getting handle on cloned container %s: %s", name, err.Error())
		return nil, err
	}

	if err := newContainerHandle.SetConfigItem("lxc.console.path", "none"); err != nil {
		return nil, err
	}
	if err := newContainerHandle.SetConfigItem("lxc.tty.max", "0"); err != nil {
		return nil, err
	}
	if err := newContainerHandle.SetConfigItem("lxc.cgroup.devices.deny", "c 5:1 rwm"); err != nil {
		return nil, err
	}

	return newContainerHandle, err
}

// housekeeper handles the needed process of handling internal logic
// in maintaining the provided lxc.Container.
func (c *containerMeta) housekeeper(ctx context.Context) {
	// container lifetime function
	log.Infof("Housekeeper (%s) started.", c.name)
	defer log.Infof("Housekeeper (%s) stopped.", c.name)

	for {
		select {
		case <-ctx.Done():
			log.Infof("Container %s: stopping", c.name)
			c.c.Stop()
			return
		case <-time.After(30 * time.Second):
			if time.Since(c.idle) > time.Duration(2*time.Minute) {
				log.Debugf("Container %s: idle for %s, stopping", c.name, time.Now().Sub(c.idle).String())
				c.c.Stop()
				c.d.activeContainers.Delete(c.name)

				if !c.c.Running() {
					return
				} else {
					log.Errorf("Container %s still running after stop call")
				}
			}
		}
	}
}

func (c *containerMeta) start() error {
	log.Debugf("Starting Container %s", c.name)

	c.idle = time.Now()
	go c.housekeeper(context.Background())

	err := c.c.WantDaemonize(true)
	if err != nil {
		return err
	}

	err = c.c.Start()
	if err != nil {
		return err
	}
	return nil
}

func (c *containerMeta) getNetwork() error {
	retries := 0
	for {
		ip, err := c.c.IPAddress("eth0")
		if err == nil {
			//log.Debugf("Got ip: %s", ip[0])
			c.ip = net.ParseIP(ip[0])
			break
		}

		if retries < 50 {
			time.Sleep(time.Millisecond * 200)
			retries++
			continue
		}

		return fmt.Errorf("Could not get an IP address for %s: %s", c.name, err.Error())
	}

	var isets []string
	netws := c.c.ConfigItem("lxc.net")
	for ind := range netws {
		itypes := c.c.RunningConfigItem(fmt.Sprintf("lxc.net.0.%d.type", ind))
		if itypes == nil {
			continue
		}

		if itypes[0] == "veth" {
			isets = c.c.RunningConfigItem(fmt.Sprintf("lxc.net.0.%d.veth.pair", ind))
			break
		} else {
			isets = c.c.RunningConfigItem(fmt.Sprintf("lxc.net.0.%d.link", ind))
			break
		}
	}

	if len(isets) == 0 {
		return fmt.Errorf("could not get an network device")
	}

	c.idevice = isets[0]

	c.idle = time.Now()

	return nil
}

func (d *trackDirector) checkExists(name string) (*lxc.Container, bool) {
	containerHandle, err := lxc.NewContainer(name)
	if err != nil {
		return nil, false
	}

	if containerHandle.Defined() {
		return containerHandle, true
	}

	return nil, false
}

type trackDirector struct {
	Template         string
	activeContainers *syncmap.Map // map[string]*lxcContainer
}

type containerMeta struct {
	c *lxc.Container
	m sync.Mutex

	d    *trackDirector
	name string

	idle     time.Time
	ip       net.IP
	idevice  string
	template string
}

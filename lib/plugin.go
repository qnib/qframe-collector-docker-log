package qframe_collector_docker_log

import (
	"bufio"
	"fmt"
	"strings"
	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"
	"github.com/zpatrick/go-config"
	"github.com/qnib/qframe-types"
	"context"
)

const (
	version = "0.0.0"
	pluginTyp = "collector"
	dockerAPI = "v1.29"
)

var (
	ctx = context.Background()
)

// struct to keep info and channels to goroutine
// -> get heartbeats so that we know it's still alive
// -> allow for gracefully shutdown of the supervisor
type ContainerSupervisor struct {
	Plugin
	CntID 		string 			 // ContainerID
	CntName 	string			 // sanatized name of container
	Container 	types.ContainerJSON
	Com 		chan interface{} // Channel to communicate with goroutine
	cli 		*client.Client
	qChan 		qtypes.QChan
}


func (p *Plugin) StartSupervisor(CntID, CntName string) {
	s := ContainerSupervisor{
		Plugin: *p,
		CntID: CntID,
		CntName: CntName,
		Com: make(chan interface{}),
		cli: p.cli,
		qChan: p.QChan,
	}
	p.sMap[CntID] = s
	go s.Run()
}

func (p *Plugin) StartSupervisorCE(ce qtypes.ContainerEvent) {
	p.StartSupervisor(ce.Event.Actor.ID, ce.Event.Actor.Attributes["name"])
}

func (cs ContainerSupervisor) Run() {
	msg := fmt.Sprintf("Start listener for: '%s' [%s]", cs.CntName, cs.CntID)
	cs.Log("notice", msg)

	reader, err := cs.cli.ContainerLogs(ctx, cs.CntID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		msg := fmt.Sprintf("Error when connecting to log of %s: %s", cs.CntName, err.Error())
		cs.Log("error", msg)
	}
	scanner := bufio.NewScanner(reader)
    for scanner.Scan() {
		base := qtypes.NewBase(cs.Name)
        qm := qtypes.NewMessage(base, cs.Name, qtypes.MsgDLOG, scanner.Text())
		cs.qChan.Data.Send(qm)
    }
	err = scanner.Err()
	if err != nil {
		msg := fmt.Sprintf("Something went wrong going through the log: %s", err.Error())
		cs.Log("error", msg)
		return
	}
	for {
		select {
		case msg := <-cs.Com:
			switch msg {
			case "died":
				msg := fmt.Sprintf("Container [%s]->'%s' died -> BYE!", cs.CntID, cs.CntName)
				cs.Log("debug", msg)
				return
			default:
				msg := fmt.Sprintf("Container [%s]->'%s' got message from cs.Com: %v", cs.CntID, cs.CntName, msg)
				cs.Log("debug", msg)
			}
		}
	}
}

type Plugin struct {
	qtypes.Plugin
	cli *client.Client
	sMap map[string]ContainerSupervisor
}

func New(qChan qtypes.QChan, cfg config.Config, name string) (Plugin, error) {
	var err error
	p := Plugin{
		Plugin: qtypes.NewNamedPlugin(qChan, cfg, pluginTyp, name, version),
		sMap: map[string]ContainerSupervisor{},
	}
	return p, err
}

func (p *Plugin) Run() {
	var err error
	dockerHost := p.CfgStringOr("docker-host", "unix:///var/run/docker.sock")
	p.cli, err = client.NewClient(dockerHost, dockerAPI, nil, nil)
	if err != nil {
		p.Log("error", fmt.Sprintf("Could not connect docker/docker/client to '%s': %v", dockerHost, err))
		return
	}
	info, err := p.cli.Info(ctx)
	if err != nil {
		p.Log("error", fmt.Sprintf("Error during Info(): %v >err> %s", info, err))
		return
	} else {
		p.Log("notice", fmt.Sprintf("Connected to '%s' / v'%s' (SWARM: %s)", info.Name, info.ServerVersion, info.Swarm.LocalNodeState))
	}
	// need to start listener for all containers
	skipRunning := p.CfgBoolOr("skip-running", false)
	if ! skipRunning {
		p.Log("info", fmt.Sprintf("Start listeners for already running containers: %d", info.ContainersRunning))
		// TODO: Fire up listeners
	}
	inputs := p.GetInputs()
	srcSuccess := p.CfgBoolOr("source-success", true)
	dc := p.QChan.Data.Join()
	for {
		select {
		case msg := <-dc.Read:
			switch msg.(type) {
			case qtypes.ContainerEvent:
				ce := msg.(qtypes.ContainerEvent)
				if len(inputs) != 0 && ! ce.InputsMatch(inputs) {
					continue
				}
				if ce.SourceSuccess != srcSuccess {
					continue
				}
				if ce.Event.Type == "container" && (strings.HasPrefix(ce.Event.Action, "exec_create") || strings.HasPrefix(ce.Event.Action, "exec_start")) {
					continue
				}
				p.Log("info", fmt.Sprintf("Received: %s", ce.Message))
				switch ce.Event.Type {
				case "container":
					switch ce.Event.Action {
					case "start":
						p.Log("info", fmt.Sprintf("Container started: %s | ID:%s", ce.Container.Name, ce.Container.ID))
						p.StartSupervisorCE(ce)
					case "die":
						p.sMap[ce.Event.Actor.ID].Com <- ce.Event.Action
					}
				}
			}
		}
	}
}

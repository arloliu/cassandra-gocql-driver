package chaos

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// TopoOps is what F-topo needs from ccm (PLAN §59.6); package cell adapts *ccmctl.Cluster to it.
type TopoOps interface {
	// AddNode adds the node to the cluster's configuration, set to bootstrap.
	AddNode(ctx context.Context, node string) error
	// NodeJoin starts the node and returns once its binary protocol is open, after its bootstrap.
	NodeJoin(ctx context.Context, node string) error
	// Decommission makes the node leave the ring; it returns once it has left.
	Decommission(ctx context.Context, node string) error
	// SaveLogs copies the node's logs into the cell's artifacts, before RemoveNode deletes its directory.
	SaveLogs(ctx context.Context, node string) error
	// RemoveNode stops the node if it runs and deletes it from the cluster.
	RemoveNode(ctx context.Context, node string) error
	// Ring reads the ring from a node that stays: each address's two-letter state, e.g. UN.
	Ring(ctx context.Context) (map[string]string, error)
	// Status reads ccm status: each node's UP or DOWN.
	Status(ctx context.Context) (map[string]string, error)
}

// TopoStep is one F-topo step, for the topo event.
type TopoStep struct {
	// Step is add, join, UN, decommission, gone, logs or remove.
	Step string `json:"step"`
	// Took is how long the step took.
	Took time.Duration `json:"took"`
}

// TopoFault adds a fourth node, waits for it to join, then decommissions and removes it (F-topo, PLAN §59.6).
type TopoFault struct {
	ops        TopoOps
	node, addr string
	driver     func() (present, up bool)
	onStep     func(TopoStep)
	added      bool
	started    bool
	installed  bool
}

var _ Fault = (*TopoFault)(nil)

// NewTopoFault returns an F-topo fault.
//
// Parameters:
//   - ops: ccm
//   - node: the node name, node4
//   - addr: its address, 127.0.1.4
//   - driver: whether the primary session lists the node, and whether it is UP
//   - onStep: receives each step with its duration; may be nil
//
// Returns:
//   - *TopoFault: the fault
func NewTopoFault(ops TopoOps, node, addr string, driver func() (present, up bool), onStep func(TopoStep)) *TopoFault {
	return &TopoFault{ops: ops, node: node, addr: addr, driver: driver, onStep: onStep}
}

// Install adds the node and starts it; the effect holds once the ring shows it UN and the driver lists it UP.
//
// Parameters:
//   - ctx: the install bound
//
// Returns:
//   - error: a failed add or start, or the effect not observed before ctx ends
func (f *TopoFault) Install(ctx context.Context) error {
	if err := f.step("add", func() error { return f.ops.AddNode(ctx, f.node) }); err != nil {
		return err
	}
	f.added = true
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("before %s started: %w", f.node, err)
	}
	f.started = true
	if err := f.step("join", func() error { return f.ops.NodeJoin(ctx, f.node) }); err != nil {
		return err
	}
	err := f.step("UN", func() error {
		return poll(ctx, func() error {
			ring, err := f.ops.Ring(ctx)
			if err != nil {
				return err
			}
			if s := ring[f.addr]; s != "UN" {
				return fmt.Errorf("%s is %q in the ring, want UN", f.addr, s)
			}
			if present, up := f.driver(); !present || !up {
				return fmt.Errorf("the driver lists %s present=%v up=%v, want both", f.addr, present, up)
			}
			return nil
		})
	})
	f.installed = err == nil
	return err
}

// Hold waits; F-topo has no active period.
//
// Parameters:
//   - ctx: the lifecycle's outer bound
//   - d: the active duration, 0 for F-topo
//
// Returns:
//   - error: ctx's error if it ends first
func (f *TopoFault) Hold(ctx context.Context, d time.Duration) error { return Sleep(ctx, d) }

// Remove decommissions the node, waits until neither the ring nor the driver lists it, saves its logs, then removes it.
// After a failed install it removes only a node whose start was never dispatched: once a start was attempted,
// whatever its result, the node may be streaming, and the cell's cluster removal is the only safe end for it
// (PLAN §5.2, §59.6). A failed add is not undone either: ccm may have written part of the node's configuration,
// which the cluster removal also ends.
//
// Parameters:
//   - ctx: the remove bound
//
// Returns:
//   - error: after a failed install, an error naming the node left in place once its start was attempted, or the
//     removal's error for a node never started; after a successful one, a failed decommission, the node still
//     listed by the ring or the driver when ctx ends, a failed log save or removal, or the node still in ccm status
func (f *TopoFault) Remove(ctx context.Context) error {
	if !f.installed {
		switch {
		case !f.added:
			return nil
		case !f.started:
			return f.ops.RemoveNode(ctx, f.node)
		}
		return fmt.Errorf("%s started, so it is left in place for the cluster's removal", f.node)
	}
	if err := f.step("decommission", func() error { return f.ops.Decommission(ctx, f.node) }); err != nil {
		return err
	}
	err := f.step("gone", func() error {
		return poll(ctx, func() error {
			ring, err := f.ops.Ring(ctx)
			if err != nil {
				return err
			}
			if s, ok := ring[f.addr]; ok {
				return fmt.Errorf("%s is still %q in the ring", f.addr, s)
			}
			if present, _ := f.driver(); present {
				return fmt.Errorf("the driver still lists %s", f.addr)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	if err := f.step("logs", func() error { return f.ops.SaveLogs(ctx, f.node) }); err != nil {
		return err
	}
	if err := f.step("remove", func() error { return f.ops.RemoveNode(ctx, f.node) }); err != nil {
		return err
	}
	st, err := f.ops.Status(ctx)
	if err != nil {
		return err
	}
	if s, ok := st[f.node]; ok {
		return errors.New(f.node + " is still in ccm status: " + s)
	}
	return nil
}

func (f *TopoFault) step(name string, fn func() error) error {
	start := time.Now()
	err := fn()
	if err != nil {
		return fmt.Errorf("%s %s: %w", name, f.node, err)
	}
	if f.onStep != nil {
		f.onStep(TopoStep{Step: name, Took: time.Since(start)})
	}
	return nil
}

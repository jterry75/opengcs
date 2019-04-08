// +build linux

package runtimev2

import "context"

type Process struct {
	cid string
	pid int
}

func (p *Process) Signal(ctx context.Context, sig int) error {

}

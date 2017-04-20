package proto

import (
	"fmt"
)

type MType uint8

const (
	_             = iota // skip 0
	MTypeOK MType = iota
	MTypeHello
	MTypeJob
	MTypeUpdate
	MTypeCancel
	MTypeList
	MTypeStop
	MTypeError
	MTypeReset
)

const maxDataSize = (1 << 32) - 1

func (mt MType) String() string {
	switch mt {
	case MTypeOK:
		return "OK"
	case MTypeHello:
		return "Hello"
	case MTypeJob:
		return "Job"
	case MTypeUpdate:
		return "Update"
	case MTypeCancel:
		return "Cancel"
	case MTypeList:
		return "List"
	case MTypeStop:
		return "Stop"
	case MTypeError:
		return "Error"
	case MTypeReset:
		return "Reset"
	default:
		return fmt.Sprintf("MType(0x%02X)", uint8(mt))
	}
}

func (mt MType) GoString() string {
	switch mt {
	case MTypeOK:
		return "MType_OK"
	case MTypeHello:
		return "MType_Hello"
	case MTypeJob:
		return "MType_Job"
	case MTypeUpdate:
		return "MType_Update"
	case MTypeCancel:
		return "MType_Cancel"
	case MTypeStop:
		return "MType_Stop"
	case MTypeList:
		return "MType_List"
	case MTypeError:
		return "MType_Error"
	case MTypeReset:
		return "MType_Reset"
	default:
		return fmt.Sprintf("MType(0x%02X)", uint8(mt))
	}
}

type MError uint8

const (
	MErrOK MError = iota
	MErrOverflow
	MErrNoProcs
)

const StopImmed = ^uint32(0)

type header struct {
	mtype   MType
	errcode MError
	seq     uint32
	padding uint32
	arg0    uint32
}

type mtErrorData struct {
	Errcode MError
	Arg0    uint32
}

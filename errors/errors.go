// Package errors provides simple error handling primitives.
//
// The traditional error handling idiom in Go is roughly akin to
//
//     if err != nil {
//             return err
//     }
//
// which when applied recursively up the call stack results in error reports
// without context or debugging information. The errors package allows
// programmers to add context to the failure path in their code in a way
// that does not destroy the original value of the error.
//
// Adding context to an error
//
// The errors.Wrap function returns a new error that adds context to the
// original error by recording a stack trace at the point Wrap is called,
// together with the supplied message. For example
//
//     _, err := ioutil.ReadAll(r)
//     if err != nil {
//             return errors.Wrap(err, "read failed")
//     }
//
// If additional control is required, the errors.WithStack and
// errors.WithMessage functions destructure errors.Wrap into its component
// operations: annotating an error with a stack trace and with a message,
// respectively.
//
// Retrieving the cause of an error
//
// Using errors.Wrap constructs a stack of errors, adding context to the
// preceding error. Depending on the nature of the error it may be necessary
// to reverse the operation of errors.Wrap to retrieve the original error
// for inspection. Any error value which implements this interface
//
//     type causer interface {
//             Cause() error
//     }
//
// can be inspected by errors.Cause. errors.Cause will recursively retrieve
// the topmost error that does not implement causer, which is assumed to be
// the original cause. For example:
//
//     switch err := errors.Cause(err).(type) {
//     case *MyError:
//             // handle specifically
//     default:
//             // unknown error
//     }
//
// Although the causer interface is not exported by this package, it is
// considered a part of its sBlocks public interface.
//
// Formatted printing of errors
//
// All error values returned from this package implement fmt.Formatter and can
// be formatted by the fmt package. The following verbs are supported:
//
//     %s    print the error. If the error has a Cause it will be
//           printed recursively.
//     %v    see %s
//     %+v   extended format. Each Frame of the error's StackTrace will
//           be printed in detail.
//
// Retrieving the stack trace of an error or wrapper
//
// New, Errorf, Wrap, and Wrapf record a stack trace at the point they are
// invoked. This information can be retrieved with the following interface:
//
//     type stackTracer interface {
//             StackTrace() errors.StackTrace
//     }
//
// The returned errors.StackTrace type is defined as
//
//     type StackTrace []Frame
//
// The Frame type represents a call site in the stack trace. Frame supports
// the fmt.Formatter interface that can be used for printing information about
// the stack trace of this error. For example:
//
//     if err, ok := err.(stackTracer); ok {
//             for _, f := range err.StackTrace() {
//                     fmt.Printf("%+s:%d\n", f, f)
//             }
//     }
//
// Although the stackTracer interface is not exported by this package, it is
// considered a part of its sBlocks public interface.
//
// See the documentation for Frame.Format for more details.
package errors

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strconv"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// Executor error codes.

	// CodeUnknown is for errors of unknown reason.
	CodeUnknown ErrCode = -1
	// CodeExecResultIsEmpty indicates execution result is empty.
	CodeExecResultIsEmpty ErrCode = 3

	// Expression error codes.

	// CodeMissConnectionID indicates connection id is missing.
	CodeMissConnectionID ErrCode = 1

	// Special error codes.

	// CodeResultUndetermined indicates the sql execution result is undetermined.
	CodeResultUndetermined ErrCode = 2
)

// ErrClass represents a class of errors.
type ErrClass int

// Error classes.
const (
	ClassAutoid ErrClass = iota + 1
	ClassnoedbS
	ClassNamespace
	ClassEvaluator
	ClassExecutor
	ClassExpression
	ClassAdmin
	Classekv
	ClassMeta
	ClassOptimizer
	ClassParser
	ClassPerfSchema
	ClassPrivilege
	ClassSchema
	ClassServer
	ClassStructure
	ClassVariable
	ClassXEval
	ClassBlocks
	ClassTypes
	ClassGlobal
	ClassMockTiekv
	ClassJSON
	ClassTiekv
	ClassCausetNet
	// Add more as needed.
)

var errClz2Str = map[ErrClass]string{
	ClassAutoid:     "autoid",
	ClassnoedbS:        "noedbS",
	ClassNamespace:     "namespace",
	ClassExecutor:   "executor",
	ClassExpression: "expression",
	ClassAdmin:      "admin",
	ClassMeta:       "meta",
	Classekv:         "ekv",
	ClassOptimizer:  "planner",
	ClassParser:     "parser",
	ClassPerfSchema: "perfschema",
	ClassPrivilege:  "privilege",
	ClassSchema:     "schema",
	ClassServer:     "server",
	ClassStructure:  "structure",
	ClassVariable:   "variable",
	ClassBlocks:      "Blocks",
	ClassTypes:      "types",
	ClassGlobal:     "global",
	ClassMockTiekv:   "mocktiekv",
	ClassJSON:       "json",
	ClassTiekv:       "tiekv",
	ClassCausetNet:    "CausetNet",
}

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	if s, exists := errClz2Str[ec]; exists {
		return s
	}
	return strconv.Itoa(int(ec))
}

// EqualClass returns true if err is *Error with the same class.
func (ec ErrClass) EqualClass(err error) bool {
	e := errors.Cause(err)
	if e == nil {
		return false
	}
	if te, ok := e.(*Error); ok {
		return te.class == ec
	}
	return false
}

// NotEqualClass returns true if err is not *Error with the same class.
func (ec ErrClass) NotEqualClass(err error) bool {
	return !ec.EqualClass(err)
}

// New creates an *Error with an error code and an error message.
// Usually used to create base *Error.
func (ec ErrClass) New(code ErrCode, message string) *Error {
	return &Error{
		class:   ec,
		code:    code,
		message: message,
	}
}

// Error implements error interface and adds integer Class and Code, so
// errors with different message can be compared.
type Error struct {
	class   ErrClass
	code    ErrCode
	message string
	args    []interface{}
	file    string
	line    int
}

// Class returns ErrClass
func (e *Error) Class() ErrClass {
	return e.class
}

// Code returns ErrCode
func (e *Error) Code() ErrCode {
	return e.code
}

// MarshalJSON implements json.Marshaler interface.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Class ErrClass `json:"class"`
		Code  ErrCode  `json:"code"`
		Msg   string   `json:"message"`
	}{
		Class: e.class,
		Code:  e.code,
		Msg:   e.getMsg(),
	})
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (e *Error) UnmarshalJSON(data []byte) error {
	err := &struct {
		Class ErrClass `json:"class"`
		Code  ErrCode  `json:"code"`
		Msg   string   `json:"message"`
	}{}

	if err := json.Unmarshal(data, &err); err != nil {
		return errors.Trace(err)
	}

	e.class = err.Class
	e.code = err.Code
	e.message = err.Msg
	return nil
}

// Location returns the location where the error is created,
// implements juju/errors locationer interface.
func (e *Error) Location() (file string, line int) {
	return e.file, e.line
}

// Error implements error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("[%s:%d]%s", e.class, e.code, e.getMsg())
}

func (e *Error) getMsg() string {
	if len(e.args) > 0 {
		return fmt.Sprintf(e.message, e.args...)
	}
	return e.message
}

// Gen generates a new *Error with the same class and code, and a new formatted message.
func (e *Error) Gen(format string, args ...interface{}) *Error {
	err := *e
	err.message = format
	err.args = args
	_, err.file, err.line, _ = runtime.Caller(1)
	return &err
}

// GenByArgs generates a new *Error with the same class and code, and new arguments.
func (e *Error) GenByArgs(args ...interface{}) *Error {
	err := *e
	err.args = args
	_, err.file, err.line, _ = runtime.Caller(1)
	return &err
}

// FastGen generates a new *Error with the same class and code, and a new formatted message.
// This will not call runtime.Caller to get file and line.
func (e *Error) FastGen(format string, args ...interface{}) *Error {
	err := *e
	err.message = format
	err.args = args
	return &err
}

// Equal checks if err is equal to e.
func (e *Error) Equal(err error) bool {
	originErr := errors.Cause(err)
	if originErr == nil {
		return false
	}

	if error(e) == originErr {
		return true
	}
	inErr, ok := originErr.(*Error)
	return ok && e.class == inErr.class && e.code == inErr.code
}

// NotEqual checks if err is not equal to e.
func (e *Error) NotEqual(err error) bool {
	return !e.Equal(err)
}

// ToSQLError convert Error to mysql.SQLError.
func (e *Error) ToSQLError() *mysql.SQLError {
	code := e.getMySQLErrorCode()
	return mysql.NewErrf(code, "%s", e.getMsg())
}

var defaultMySQLErrorCode uint16

func (e *Error) getMySQLErrorCode() uint16 {
	codeMap, ok := ErrClassToMySQLCodes[e.class]
	if !ok {
		log.Warnf("Unknown error class: %v", e.class)
		return defaultMySQLErrorCode
	}
	code, ok := codeMap[e.code]
	if !ok {
		log.Debugf("Unknown error class: %v code: %v", e.class, e.code)
		return defaultMySQLErrorCode
	}
	return code
}

var (
	// ErrClassToMySQLCodes is the map of ErrClass to code-map.
	ErrClassToMySQLCodes map[ErrClass]map[ErrCode]uint16
)

func init() {
	ErrClassToMySQLCodes = make(map[ErrClass]map[ErrCode]uint16)
	defaultMySQLErrorCode = mysql.ErrUnknown
}

// ErrorEqual returns a boolean indicating whether err1 is equal to err2.
func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	te1, ok1 := e1.(*Error)
	te2, ok2 := e2.(*Error)
	if ok1 && ok2 {
		return te1.class == te2.class && te1.code == te2.code
	}

	return e1.Error() == e2.Error()
}

// ErrorNotEqual returns a boolean indicating whether err1 isn't equal to err2.
func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}

// MustNil fatals if err is not nil.
func MustNil(err error) {
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}
}

// Call executes a function and checks the returned err.
func Call(fn func() error) {
	err := fn()
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

// Log logs the error if it is not nil.
func Log(err error) {
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

// New returns an error with the supplied message.
// New also records the stack trace at the point it was called.
func New(message string) error {
	return &fundamental{
		msg:   message,
		stack: callers(),
	}
}

// Errorf formats according to a format specifier and returns the string
// as a value that satisfies error.
// Errorf also records the stack trace at the point it was called.
func Errorf(format string, args ...interface{}) error {
	return &fundamental{
		msg:   fmt.Sprintf(format, args...),
		stack: callers(),
	}
}

// fundamental is an error that has a message and a stack, but no caller.
type fundamental struct {
	msg string
	*stack
}

func (f *fundamental) Error() string { return f.msg }

func (f *fundamental) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, f.msg)
			f.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, f.msg)
	case 'q':
		fmt.Fprintf(s, "%q", f.msg)
	}
}

// WithStack annotates err with a stack trace at the point WithStack was called.
// If err is nil, WithStack returns nil.
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	return &withStack{
		err,
		callers(),
	}
}

type withStack struct {
	error
	*stack
}

func (w *withStack) Cause() error { return w.error }

func (w *withStack) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.Cause())
			w.stack.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   message,
	}
	return &withStack{
		err,
		callers(),
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is called, and the format specifier.
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
	return &withStack{
		err,
		callers(),
	}
}

// WithMessage annotates err with a new message.
// If err is nil, WithMessage returns nil.
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   message,
	}
}

// WithMessagef annotates err with the format specifier.
// If err is nil, WithMessagef returns nil.
func WithMessagef(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
}

type withMessage struct {
	cause error
	msg   string
}

func (w *withMessage) Error() string { return w.msg + ": " + w.cause.Error() }
func (w *withMessage) Cause() error  { return w.cause }

func (w *withMessage) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n", w.Cause())
			io.WriteString(s, w.msg)
			return
		}
		fallthrough
	case 's', 'q':
		io.WriteString(s, w.Error())
	}
}

// Cause returns the underlying cause of the error, if possible.
// An error value has a cause if it implements the following
// interface:
//
//     type causer interface {
//            Cause() error
//     }
//
// If the error does not implement Cause, the original error will
// be returned. If the error is nil, nil will be returned without further
// investigation.
func Cause(err error) error {
	type causer interface {
		Cause() error
	}

	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return err
}

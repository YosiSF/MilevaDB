package config

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/milevadb/errors"
	tracing "github.com/uber/jaeger-client-go/config"
	"github.com/milevadb/util/hyperlogutil"
	"go.uber.org/atomic"

)

//limit the number of log sizes 

const (
	MaxLogFileSize = 4096 // MB
)
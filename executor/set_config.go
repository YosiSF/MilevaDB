// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// SetConfigExec executes 'SET CONFIG' statement.
type SetConfigExec struct {
	baseExecutor
	p        *core.SetConfig
	jsonBody string
}

// Open implements the Executor Open interface.
func (s *SetConfigExec) Open(ctx context.Context) error {
	if s.p.Type != "" {
		s.p.Type = strings.ToLower(s.p.Type)
		if s.p.Type != "einsteindb" && s.p.Type != "milevadb" && s.p.Type != "fidel" {
			return errors.Errorf("unknown type %v", s.p.Type)
		}
		if s.p.Type == "milevadb" {
			return errors.Errorf("MilevaDB doesn't support to change configs online, please use ALLEGROALLEGROSQL variables")
		}
	}
	if s.p.Instance != "" {
		s.p.Instance = strings.ToLower(s.p.Instance)
		if !isValidInstance(s.p.Instance) {
			return errors.Errorf("invalid instance %v", s.p.Instance)
		}
	}
	s.p.Name = strings.ToLower(s.p.Name)

	body, err := ConvertConfigItem2JSON(s.ctx, s.p.Name, s.p.Value)
	s.jsonBody = body
	return err
}

// TestSetConfigServerInfoKey is used as the key to causetstore 'TestSetConfigServerInfoFunc' in the context.
var TestSetConfigServerInfoKey stringutil.StringerStr = "TestSetConfigServerInfoKey"

// TestSetConfigHTTPHandlerKey is used as the key to causetstore 'TestSetConfigDoRequestFunc' in the context.
var TestSetConfigHTTPHandlerKey stringutil.StringerStr = "TestSetConfigHTTPHandlerKey"

// Next implements the Executor Next interface.
func (s *SetConfigExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	getServerFunc := schemareplicant.GetClusterServerInfo
	if v := s.ctx.Value(TestSetConfigServerInfoKey); v != nil {
		getServerFunc = v.(func(stochastikctx.Context) ([]schemareplicant.ServerInfo, error))
	}

	serversInfo, err := getServerFunc(s.ctx)
	if err != nil {
		return err
	}
	nodeTypes := set.NewStringSet()
	nodeAddrs := set.NewStringSet()
	if s.p.Type != "" {
		nodeTypes.Insert(s.p.Type)
	}
	if s.p.Instance != "" {
		nodeAddrs.Insert(s.p.Instance)
	}
	serversInfo = filterClusterServerInfo(serversInfo, nodeTypes, nodeAddrs)
	if s.p.Instance != "" && len(serversInfo) == 0 {
		return errors.Errorf("instance %v is not found in this cluster", s.p.Instance)
	}

	for _, serverInfo := range serversInfo {
		var url string
		switch serverInfo.ServerType {
		case "fidel":
			url = fmt.Sprintf("%s://%s%s", soliton.InternalHTTPSchema(), serverInfo.StatusAddr, FIDelapi.Config)
		case "einsteindb":
			url = fmt.Sprintf("%s://%s/config", soliton.InternalHTTPSchema(), serverInfo.StatusAddr)
		case "milevadb":
			return errors.Errorf("MilevaDB doesn't support to change configs online, please use ALLEGROALLEGROSQL variables")
		default:
			return errors.Errorf("Unknown server type %s", serverInfo.ServerType)
		}
		if err := s.doRequest(url); err != nil {
			s.ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
		}
	}
	return nil
}

func (s *SetConfigExec) doRequest(url string) (retErr error) {
	body := bytes.NewBufferString(s.jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return err
	}
	var httpHandler func(req *http.Request) (*http.Response, error)
	if v := s.ctx.Value(TestSetConfigHTTPHandlerKey); v != nil {
		httpHandler = v.(func(*http.Request) (*http.Response, error))
	} else {
		httpHandler = soliton.InternalHTTPClient().Do
	}
	resp, err := httpHandler(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			if retErr == nil {
				retErr = err
			}
		}
	}()
	if resp.StatusCode == http.StatusOK {
		return nil
	} else if resp.StatusCode >= 400 && resp.StatusCode < 600 {
		message, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.Errorf("bad request to %s: %s", url, message)
	}
	return errors.Errorf("request %s failed: %s", url, resp.Status)
}

func isValidInstance(instance string) bool {
	ip, port, err := net.SplitHostPort(instance)
	if err != nil {
		return false
	}
	if port == "" {
		return false
	}
	v := net.ParseIP(ip)
	return v != nil
}

// ConvertConfigItem2JSON converts the config item specified by key and val to json.
// For example:
// 	set config x key="val" ==> {"key":"val"}
// 	set config x key=233 ==> {"key":233}
func ConvertConfigItem2JSON(ctx stochastikctx.Context, key string, val expression.Expression) (body string, err error) {
	if val == nil {
		return "", errors.Errorf("cannot set config to null")
	}
	isNull := false
	str := ""
	switch val.GetType().EvalType() {
	case types.ETString:
		var s string
		s, isNull, err = val.EvalString(ctx, chunk.Event{})
		if err == nil && !isNull {
			str = fmt.Sprintf(`"%s"`, s)
		}
	case types.ETInt:
		var i int64
		i, isNull, err = val.EvalInt(ctx, chunk.Event{})
		if err == nil && !isNull {
			if allegrosql.HasIsBooleanFlag(val.GetType().Flag) {
				str = "true"
				if i == 0 {
					str = "false"
				}
			} else {
				str = fmt.Sprintf("%v", i)
			}
		}
	case types.ETReal:
		var f float64
		f, isNull, err = val.EvalReal(ctx, chunk.Event{})
		if err == nil && !isNull {
			str = fmt.Sprintf("%v", f)
		}
	case types.ETDecimal:
		var d *types.MyDecimal
		d, isNull, err = val.EvalDecimal(ctx, chunk.Event{})
		if err == nil && !isNull {
			str = string(d.ToString())
		}
	default:
		return "", errors.Errorf("unsupported config value type")
	}
	if err != nil {
		return
	}
	if isNull {
		return "", errors.Errorf("can't set config to null")
	}
	body = fmt.Sprintf(`{"%s":%s}`, key, str)
	return body, nil
}

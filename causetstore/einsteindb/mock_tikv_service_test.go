package einsteindb

import (
	"fmt"
	"net"
	"time"

	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type server struct {
	einsteindbpb.EinsteinDBServer
}

func (s *server) BatchCommands(ss einsteindbpb.EinsteinDB_BatchCommandsServer) error {
	for {
		req, err := ss.Recv()
		if err != nil {
			logutil.BgLogger().Error("batch commands receive fail", zap.Error(err))
			return err
		}

		responses := make([]*einsteindbpb.BatchCommandsResponse_Response, 0, len(req.GetRequestIds()))
		for i := 0; i < len(req.GetRequestIds()); i++ {
			responses = append(responses, &einsteindbpb.BatchCommandsResponse_Response{
				Cmd: &einsteindbpb.BatchCommandsResponse_Response_Empty{
					Empty: &einsteindbpb.BatchCommandsEmptyResponse{},
				},
			})
		}

		err = ss.Send(&einsteindbpb.BatchCommandsResponse{
			Responses:  responses,
			RequestIds: req.GetRequestIds(),
		})
		if err != nil {
			logutil.BgLogger().Error("batch commands send fail", zap.Error(err))
			return err
		}
	}
}

// Try to start a gRPC server and retrun the server instance and binded port.
func startMockEinsteinDBService() (*grpc.Server, int) {
	port := -1
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "127.0.0.1", 0))
	if err != nil {
		logutil.BgLogger().Error("can't listen", zap.Error(err))
		logutil.BgLogger().Error("can't start mock einsteindb service because no available ports")
		return nil, port
	}
	port = lis.Addr().(*net.TCPAddr).Port
	s := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
	einsteindbpb.RegisterEinsteinDBServer(s, &server{})
	go func() {
		if err = s.Serve(lis); err != nil {
			logutil.BgLogger().Error(
				"can't serve gRPC requests",
				zap.Error(err),
			)
		}
	}()
	return s, port
}

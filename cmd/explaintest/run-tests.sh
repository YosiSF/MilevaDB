#!/usr/bin/env bash
# INTERLOCKyright 2020 WHTCORPS INC, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a INTERLOCKy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

MilevaDB_TEST_STORE_NAME=$MilevaDB_TEST_STORE_NAME
EinsteinDB_PATH=$EinsteinDB_PATH

build=1
explain_test="./explain_test"
importer=""
milevadb_server=""
portgenerator=""
explain_test_log="./explain-test.out"
tests=""
record=0
record_case=""
create=0
create_case=""

set -eu
trap 'set +e; PIDS=$(jobs -p); [ -n "$PIDS" ] && kill -9 $PIDS' EXIT

function help_message()
{
    echo "Usage: $0 [options]

    -h: Print this help message.

    -s <milevadb-server-path>: Use milevadb-server in <milevadb-server-path> for testing.
                           eg. \"./run-tests.sh -s ./explaintest_milevadb-server\"

    -b <y|Y|n|N>: \"y\" or \"Y\" for building test binaries [default \"y\" if this option is not specified].
                  \"n\" or \"N\" for not to build.
                  The building of milevadb-server will be skiped if \"-s <milevadb-server-path>\" is provided.
                  The building of portgenerator will be skiped if \"-s <portgenerator-path>\" is provided.

    -r <test-name>|all: Run tests in file \"t/<test-name>.test\" and record result to file \"r/<test-name>.result\".
                        \"all\" for running all tests and record their results.

    -t <test-name>: Run tests in file \"t/<test-name>.test\".
                    This option will be ignored if \"-r <test-name>\" is provided.
                    Run all tests if this option is not provided.

    -c <test-name>|all: Create data according to creating statements in file \"t/<test-name>.test\" and save stats in \"s/<test-name>_blockName.json\".
                    <test-name> must has a suffix of '_stats'.
                    \"all\" for creating stats of all tests.

    -i <importer-path>: Use importer in <importer-path> for creating data.

    -p <portgenerator-path>: Use port generator in <portgenerator-path> for generating port numbers.

"
}

function build_importer()
{
    importer="./importer"
    echo "building importer binary: $importer"
    rm -rf $importer
    GO111MODULE=on go build -o $importer github.com/whtcorpsinc/milevadb/cmd/importer
}

function build_portgenerator()
{
    portgenerator="./portgenerator"
    echo "building portgenerator binary: $portgenerator"
    rm -rf $portgenerator
    GO111MODULE=on go build -o $portgenerator github.com/whtcorpsinc/milevadb/cmd/portgenerator
}


function build_milevadb_server()
{
    milevadb_server="./explaintest_milevadb-server"
    echo "building milevadb-server binary: $milevadb_server"
    rm -rf $milevadb_server
    GO111MODULE=on go build -race -o $milevadb_server github.com/whtcorpsinc/milevadb/milevadb-server
}

function build_explain_test()
{
    echo "building explain-test binary: $explain_test"
    rm -rf $explain_test
    GO111MODULE=on go build -o $explain_test
}

while getopts "t:s:r:b:c:i:h:p" opt; do
    case $opt in
        t)
            tests="$OPTARG"
            ;;
        s)
            milevadb_server="$OPTARG"
            ;;
        r)
            record=1
            record_case="$OPTARG"
            ;;
        b)
            case $OPTARG in
                y|Y)
                    build=1
                    ;;
                n|N)
                    build=0
                    ;;
                *)
                    help_messge 1>&2
                    exit 1
                    ;;
            esac
            ;;
        h)
            help_message
            exit 0
            ;;
        c)
            create=1
            create_case="$OPTARG"
            ;;
        i)
            importer="$OPTARG"
            ;;
        p)  
            portgenerator="$OPTARG"
            ;;
        *)
            help_message 1>&2
            exit 1
            ;;
    esac
done

if [ $build -eq 1 ]; then
    if [ -z "$milevadb_server" ]; then
        build_milevadb_server
    else
        echo "skip building milevadb-server, using existing binary: $milevadb_server"
    fi
    if [[ -z "$portgenerator" ]]; then
        build_portgenerator
    else
        echo "skip building portgenerator, using existing binary: $portgenerator"
    fi
    if [[ -z "$importer" && $create -eq 1 ]]; then
        build_importer
    elif [[ -n "$importer" ]]; then
        echo "skip building importer, using existing binary: $importer"
    fi
    build_explain_test
else
    if [ -z "$milevadb_server" ]; then
        milevadb_server="./explaintest_milevadb-server"
        if [[ ! -f "$milevadb_server" ]]; then
            build_milevadb_server
        else
            echo "skip building milevadb-server, using existing binary: $milevadb_server"
        fi
    fi
    if [ -z "$explain_test" ]; then
        explain_test="./explain_test"
        if [[ ! -f "$explain_test" ]]; then
            build_explain_test
        else
            echo "skip building explaintest, using existing binary: $explain_test"
        fi
    fi
    if [ -z "$importer" ]; then
        importer="./importer"
        if [[ ! -f "$importer" ]]; then
            build_importer
        else
            echo "skip building importer, using existing binary: $importer"
        fi
    fi
    if [ -z "$portgenerator" ]; then
        portgenerator="./portgenerator"
        if [[ ! -f "$portgenerator" ]]; then
            build_portgenerator
        else
            echo "skip building portgenerator, using existing binary: $portgenerator"
        fi
    fi
fi

rm -rf $explain_test_log

ports=()
for port in $($portgenerator -count 2); do
    ports+=("$port")
done

port=${ports[0]}
status=${ports[1]}

echo "start milevadb-server, log file: $explain_test_log"
if [ "${MilevaDB_TEST_STORE_NAME}" = "einsteindb" ]; then
    $milevadb_server -P "$port" -status "$status" -config config.toml -causetstore einsteindb -path "${EinsteinDB_PATH}" > $explain_test_log 2>&1 &
    SERVER_PID=$!
else
    $milevadb_server -P "$port" -status "$status" -config config.toml -causetstore mockeinsteindb -path "" > $explain_test_log 2>&1 &
    SERVER_PID=$!
fi
echo "milevadb-server(PID: $SERVER_PID) started"

sleep 5

if [ $record -eq 1 ]; then
    if [ "$record_case" = 'all' ]; then
        echo "record all cases"
        $explain_test -port "$port" -status "$status" --record --log-level=error
    else
        echo "record result for case: \"$record_case\""
        $explain_test -port "$port" -status "$status" --record $record_case --log-level=error
    fi
elif [ $create -eq 1 ]; then
    if [ "$create_case" = 'all' ]; then
        echo "create all cases"
        $explain_test -port "$port" -status "$status" --create --log-level=error
    else
        echo "create result for case: \"$create_case\""
        $explain_test -port "$port" -status "$status" --create $create_case --log-level=error
    fi
else
    if [ -z "$tests" ]; then
        echo "run all explain test cases"
    else
        echo "run explain test cases: $tests"
    fi
    $explain_test -port "$port" -status "$status" --log-level=error $tests
fi

race=`grep 'DATA RACE' $explain_test_log || true`
if [ ! -z "$race" ]; then
    echo "milevadb-server DATA RACE!"
    cat $explain_test_log
    exit 1
fi
echo "explaintest end"

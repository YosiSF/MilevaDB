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

package executor_test

import (
	"fmt"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite1) TestRevokeGlobal(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON *.* FROM 'nonexistuser'@'host'`)
	c.Assert(err, NotNil)

	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testGlobalRevoke'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserALLEGROSQL)
	grantPrivALLEGROSQL := `GRANT ALL PRIVILEGES ON *.* to 'testGlobalRevoke'@'localhost';`
	tk.MustExec(grantPrivALLEGROSQL)

	// Make sure all the global privs for new user is "Y".
	for _, v := range allegrosql.AllDBPrivs {
		allegrosql := fmt.Sprintf(`SELECT %s FROM allegrosql.User WHERE User="testGlobalRevoke" and host="localhost";`, allegrosql.Priv2UserDefCaus[v])
		r := tk.MustQuery(allegrosql)
		r.Check(testkit.Events("Y"))
	}

	// Revoke each priv from the user.
	for _, v := range allegrosql.AllGlobalPrivs {
		allegrosql := fmt.Sprintf("REVOKE %s ON *.* FROM 'testGlobalRevoke'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustExec(allegrosql)
		allegrosql = fmt.Sprintf(`SELECT %s FROM allegrosql.User WHERE User="testGlobalRevoke" and host="localhost"`, allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("N"))
	}
}

func (s *testSuite1) TestRevokeDBSINTERLOCKe(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testDBRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`GRANT ALL ON test.* TO 'testDBRevoke'@'localhost';`)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON nonexistdb.* FROM 'testDBRevoke'@'localhost'`)
	c.Assert(err, NotNil)

	// Revoke each priv from the user.
	for _, v := range allegrosql.AllDBPrivs {
		check := fmt.Sprintf(`SELECT %s FROM allegrosql.EDB WHERE User="testDBRevoke" and host="localhost" and EDB="test"`, allegrosql.Priv2UserDefCaus[v])
		allegrosql := fmt.Sprintf("REVOKE %s ON test.* FROM 'testDBRevoke'@'localhost';", allegrosql.Priv2Str[v])

		tk.MustQuery(check).Check(testkit.Events("Y"))
		tk.MustExec(allegrosql)
		tk.MustQuery(check).Check(testkit.Events("N"))
	}
}

func (s *testSuite1) TestRevokeBlockSINTERLOCKe(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testTblRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`CREATE TABLE test.test1(c1 int);`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON test.test1 TO 'testTblRevoke'@'localhost';`)

	_, err := tk.Exec(`REVOKE ALL PRIVILEGES ON test.nonexistblock FROM 'testTblRevoke'@'localhost'`)
	c.Assert(err, NotNil)

	// Make sure all the block privs for new user is Y.
	res := tk.MustQuery(`SELECT Block_priv FROM allegrosql.blocks_priv WHERE User="testTblRevoke" and host="localhost" and EDB="test" and Block_name="test1"`)
	res.Check(testkit.Events("Select,Insert,UFIDelate,Delete,Create,Drop,Index,Alter"))

	// Revoke each priv from the user.
	for _, v := range allegrosql.AllBlockPrivs {
		allegrosql := fmt.Sprintf("REVOKE %s ON test.test1 FROM 'testTblRevoke'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustExec(allegrosql)
		rows := tk.MustQuery(`SELECT Block_priv FROM allegrosql.blocks_priv WHERE User="testTblRevoke" and host="localhost" and EDB="test" and Block_name="test1";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Equals, -1)
	}

	// Revoke all block sINTERLOCKe privs.
	tk.MustExec("REVOKE ALL ON test.test1 FROM 'testTblRevoke'@'localhost';")
	tk.MustQuery(`SELECT Block_priv FROM allegrosql.Blocks_priv WHERE User="testTblRevoke" and host="localhost" and EDB="test" and Block_name="test1"`).Check(testkit.Events(""))
}

func (s *testSuite1) TestRevokeDeferredCausetSINTERLOCKe(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	tk.MustExec(`CREATE USER 'testDefCausRevoke'@'localhost' IDENTIFIED BY '123';`)
	tk.MustExec(`CREATE TABLE test.test3(c1 int, c2 int);`)
	tk.MustQuery(`SELECT * FROM allegrosql.DeferredCausets_priv WHERE User="testDefCausRevoke" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c2"`).Check(testkit.Events())

	// Grant and Revoke each priv on the user.
	for _, v := range allegrosql.AllDeferredCausetPrivs {
		grantALLEGROSQL := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testDefCausRevoke'@'localhost';", allegrosql.Priv2Str[v])
		revokeALLEGROSQL := fmt.Sprintf("REVOKE %s(c1) ON test.test3 FROM 'testDefCausRevoke'@'localhost';", allegrosql.Priv2Str[v])
		checkALLEGROSQL := `SELECT DeferredCauset_priv FROM allegrosql.DeferredCausets_priv WHERE User="testDefCausRevoke" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c1"`

		tk.MustExec(grantALLEGROSQL)
		rows := tk.MustQuery(checkALLEGROSQL).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)

		tk.MustExec(revokeALLEGROSQL)
		tk.MustQuery(checkALLEGROSQL).Check(testkit.Events(""))
	}

	// Create a new user.
	tk.MustExec("CREATE USER 'testDefCaus1Revoke'@'localhost' IDENTIFIED BY '123';")
	tk.MustExec("USE test;")
	// Grant all defCausumn sINTERLOCKe privs.
	tk.MustExec("GRANT ALL(c2) ON test3 TO 'testDefCaus1Revoke'@'localhost';")
	// Make sure all the defCausumn privs for granted user are in the DeferredCauset_priv set.
	for _, v := range allegrosql.AllDeferredCausetPrivs {
		rows := tk.MustQuery(`SELECT DeferredCauset_priv FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus1Revoke" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c2";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)
	}
	tk.MustExec("REVOKE ALL(c2) ON test3 FROM 'testDefCaus1Revoke'@'localhost'")
	tk.MustQuery(`SELECT DeferredCauset_priv FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus1Revoke" and host="localhost" and EDB="test" and Block_name="test3"`).Check(testkit.Events(""))
}

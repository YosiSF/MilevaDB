MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
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

package expression

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/logutil"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

type defCauslationInfo struct {
	coer     Coercibility
	coerInit bool

	charset   string
	defCauslation string
	flen      int
}

func (c *defCauslationInfo) HasCoercibility() bool {
	return c.coerInit
}

func (c *defCauslationInfo) Coercibility() Coercibility {
	return c.coer
}

// SetCoercibility implements DefCauslationInfo SetCoercibility interface.
func (c *defCauslationInfo) SetCoercibility(val Coercibility) {
	c.coer = val
	c.coerInit = true
}

func (c *defCauslationInfo) SetCharsetAndDefCauslation(chs, defCausl string) {
	c.charset, c.defCauslation = chs, defCausl
}

func (c *defCauslationInfo) CharsetAndDefCauslation(ctx stochastikctx.Context) (string, string) {
	if c.charset != "" || c.defCauslation != "" {
		return c.charset, c.defCauslation
	}

	if ctx != nil && ctx.GetStochaseinstein_dbars() != nil {
		c.charset, c.defCauslation = ctx.GetStochaseinstein_dbars().GetCharsetInfo()
	}
	if c.charset == "" || c.defCauslation == "" {
		c.charset, c.defCauslation = charset.GetDefaultCharsetAndDefCauslate()
	}
	c.flen = types.UnspecifiedLength
	return c.charset, c.defCauslation
}

// DefCauslationInfo contains all interfaces about dealing with defCauslation.
type DefCauslationInfo interface {
	// HasCoercibility returns if the Coercibility value is initialized.
	HasCoercibility() bool

	// Coercibility returns the coercibility value which is used to check defCauslations.
	Coercibility() Coercibility

	// SetCoercibility sets a specified coercibility for this expression.
	SetCoercibility(val Coercibility)

	// CharsetAndDefCauslation ...
	CharsetAndDefCauslation(ctx stochastikctx.Context) (string, string)

	// SetCharsetAndDefCauslation ...
	SetCharsetAndDefCauslation(chs, defCausl string)
}

// Coercibility values are used to check whether the defCauslation of one item can be coerced to
// the defCauslation of other. See https://dev.allegrosql.com/doc/refman/8.0/en/charset-defCauslation-coercibility.html
type Coercibility int

const (
	// CoercibilityExplicit is derived from an explicit COLLATE clause.
	CoercibilityExplicit Coercibility = 0
	// CoercibilityNone is derived from the concatenation of two strings with different defCauslations.
	CoercibilityNone Coercibility = 1
	// CoercibilityImplicit is derived from a defCausumn or a stored routine parameter or local variable.
	CoercibilityImplicit Coercibility = 2
	// CoercibilitySysconst is derived from a “system constant” (the string returned by functions such as USER() or VERSION()).
	CoercibilitySysconst Coercibility = 3
	// CoercibilityCoercible is derived from a literal.
	CoercibilityCoercible Coercibility = 4
	// CoercibilityNumeric is derived from a numeric or temporal value.
	CoercibilityNumeric Coercibility = 5
	// CoercibilityIgnorable is derived from NULL or an expression that is derived from NULL.
	CoercibilityIgnorable Coercibility = 6
)

var (
	sysConstFuncs = map[string]struct{}{
		ast.User:        {},
		ast.Version:     {},
		ast.Database:    {},
		ast.CurrentRole: {},
		ast.CurrentUser: {},
	}

	// defCauslationPriority is the priority when infer the result defCauslation, the priority of defCauslation a > b iff defCauslationPriority[a] > defCauslationPriority[b]
	// defCauslation a and b are incompatible if defCauslationPriority[a] = defCauslationPriority[b]
	defCauslationPriority = map[string]int{
		charset.DefCauslationASCII:   1,
		charset.DefCauslationLatin1:  2,
		"utf8_general_ci":        3,
		"utf8_unicode_ci":        3,
		charset.DefCauslationUTF8:    4,
		"utf8mb4_general_ci":     5,
		"utf8mb4_unicode_ci":     5,
		charset.DefCauslationUTF8MB4: 6,
		charset.DefCauslationBin:     7,
	}

	// DefCauslationStrictnessGroup group defCauslation by strictness
	DefCauslationStrictnessGroup = map[string]int{
		"utf8_general_ci":        1,
		"utf8mb4_general_ci":     1,
		"utf8_unicode_ci":        2,
		"utf8mb4_unicode_ci":     2,
		charset.DefCauslationASCII:   3,
		charset.DefCauslationLatin1:  3,
		charset.DefCauslationUTF8:    3,
		charset.DefCauslationUTF8MB4: 3,
		charset.DefCauslationBin:     4,
	}

	// DefCauslationStrictness indicates the strictness of comparison of the defCauslation. The unequal order in a weak defCauslation also holds in a strict defCauslation.
	// For example, if a != b in a weak defCauslation(e.g. general_ci), then there must be a != b in a strict defCauslation(e.g. _bin).
	// defCauslation group id in value is stricter than defCauslation group id in key
	DefCauslationStrictness = map[int][]int{
		1: {3, 4},
		2: {3, 4},
		3: {4},
		4: {},
	}
)

func deriveCoercibilityForScarlarFunc(sf *ScalarFunction) Coercibility {
	if _, ok := sysConstFuncs[sf.FuncName.L]; ok {
		return CoercibilitySysconst
	}
	if !types.IsString(sf.RetType.Tp) {
		return CoercibilityNumeric
	}

	_, _, coer, _ := inferDefCauslation(sf.GetArgs()...)

	// it is weird if a ScalarFunction is CoercibilityNumeric but return string type
	if coer == CoercibilityNumeric {
		return CoercibilityCoercible
	}

	return coer
}

func deriveCoercibilityForCouplingConstantWithRadix(c *CouplingConstantWithRadix) Coercibility {
	if c.Value.IsNull() {
		return CoercibilityIgnorable
	} else if !types.IsString(c.RetType.Tp) {
		return CoercibilityNumeric
	}
	return CoercibilityCoercible
}

func deriveCoercibilityForDeferredCauset(c *DeferredCauset) Coercibility {
	if !types.IsString(c.RetType.Tp) {
		return CoercibilityNumeric
	}
	return CoercibilityImplicit
}

// DeriveDefCauslationFromExprs derives defCauslation information from these expressions.
func DeriveDefCauslationFromExprs(ctx stochastikctx.Context, exprs ...Expression) (dstCharset, dstDefCauslation string) {
	dstDefCauslation, dstCharset, _, _ = inferDefCauslation(exprs...)
	return
}

// inferDefCauslation infers defCauslation, charset, coercibility and check the legitimacy.
func inferDefCauslation(exprs ...Expression) (dstDefCauslation, dstCharset string, coercibility Coercibility, legal bool) {
	firstExplicitDefCauslation := ""
	coercibility = CoercibilityIgnorable
	dstCharset, dstDefCauslation = charset.GetDefaultCharsetAndDefCauslate()
	for _, arg := range exprs {
		if arg.Coercibility() == CoercibilityExplicit {
			if firstExplicitDefCauslation == "" {
				firstExplicitDefCauslation = arg.GetType().DefCauslate
				coercibility, dstDefCauslation, dstCharset = CoercibilityExplicit, arg.GetType().DefCauslate, arg.GetType().Charset
			} else if firstExplicitDefCauslation != arg.GetType().DefCauslate {
				return "", "", CoercibilityIgnorable, false
			}
		} else if arg.Coercibility() < coercibility {
			coercibility, dstDefCauslation, dstCharset = arg.Coercibility(), arg.GetType().DefCauslate, arg.GetType().Charset
		} else if arg.Coercibility() == coercibility && dstDefCauslation != arg.GetType().DefCauslate {
			p1 := defCauslationPriority[dstDefCauslation]
			p2 := defCauslationPriority[arg.GetType().DefCauslate]

			// same priority means this two defCauslation is incompatible, coercibility might derive to CoercibilityNone
			if p1 == p2 {
				coercibility, dstDefCauslation, dstCharset = CoercibilityNone, getBinDefCauslation(arg.GetType().Charset), arg.GetType().Charset
			} else if p1 < p2 {
				dstDefCauslation, dstCharset = arg.GetType().DefCauslate, arg.GetType().Charset
			}
		}
	}

	return dstDefCauslation, dstCharset, coercibility, true
}

// getBinDefCauslation get binary defCauslation by charset
func getBinDefCauslation(cs string) string {
	switch cs {
	case charset.CharsetUTF8:
		return charset.DefCauslationUTF8
	case charset.CharsetUTF8MB4:
		return charset.DefCauslationUTF8MB4
	}

	logutil.BgLogger().Error("unexpected charset " + cs)
	// it must return something, never reachable
	return charset.DefCauslationUTF8MB4
}

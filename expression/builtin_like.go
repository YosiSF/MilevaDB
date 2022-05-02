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
	"regexp"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/chunk"
	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/defCauslate"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"github.com/whtcorpsinc/MilevaDB-Prod/types"
)

var (
	_ functionClass = &likeFunctionClass{}
	_ functionClass = &regexpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
	_ builtinFunc = &builtinRegexpSig{}
	_ builtinFunc = &builtinRegexpUTF8Sig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.CausetEDN}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinLikeSig{bf, nil, false, sync.Once{}}
	sig.setPbCode(fidelpb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	baseBuiltinFunc
	// pattern and isMemorizedPattern is not serialized with builtinLikeSig, treat them as a cache to accelerate
	// the evaluation of builtinLikeSig.
	pattern            defCauslate.WildcardPattern
	isMemorizedPattern bool
	once               sync.Once
}

func (b *builtinLikeSig) Clone() builtinFunc {
	newSig := &builtinLikeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.pattern = b.pattern
	newSig.isMemorizedPattern = b.isMemorizedPattern
	return newSig
}

// evalInt evals a builtinLikeSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
func (b *builtinLikeSig) evalInt(event chunk.Event) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}

	patternStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	escape, isNull, err := b.args[2].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return 0, isNull, err
	}
	memorization := func() {
		if b.pattern == nil {
			b.pattern = b.defCauslator().Pattern()
			if b.args[1].ConstItem(b.ctx.GetStochaseinstein_dbars().StmtCtx) && b.args[2].ConstItem(b.ctx.GetStochaseinstein_dbars().StmtCtx) {
				b.pattern.Compile(patternStr, byte(escape))
				b.isMemorizedPattern = true
			}
		}
	}
	// Only be executed once to achieve thread-safe
	b.once.Do(memorization)
	if !b.isMemorizedPattern {
		// Must not use b.pattern to avoid data race
		pattern := b.defCauslator().Pattern()
		pattern.Compile(patternStr, byte(escape))
		return boolToInt64(pattern.DoMatch(valStr)), false, nil
	}
	return boolToInt64(b.pattern.DoMatch(valStr)), false, nil
}

type regexpFunctionClass struct {
	baseFunctionClass
}

func (c *regexpFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.CausetEDN, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	var sig builtinFunc
	if bf.defCauslation == charset.DefCauslationBin {
		sig = newBuiltinRegexpSig(bf)
		sig.setPbCode(fidelpb.ScalarFuncSig_RegexpSig)
	} else {
		sig = newBuiltinRegexpUTF8Sig(bf)
		sig.setPbCode(fidelpb.ScalarFuncSig_RegexpUTF8Sig)
	}
	return sig, nil
}

type builtinRegexpSharedSig struct {
	baseBuiltinFunc
	compile         func(string) (*regexp.Regexp, error)
	memorizedRegexp *regexp.Regexp
	memorizedErr    error
}

func (b *builtinRegexpSharedSig) clone(from *builtinRegexpSharedSig) {
	b.cloneFrom(&from.baseBuiltinFunc)
	b.compile = from.compile
	if from.memorizedRegexp != nil {
		b.memorizedRegexp = from.memorizedRegexp.INTERLOCKy()
	}
	b.memorizedErr = from.memorizedErr
}

// evalInt evals `expr REGEXP pat`, or `expr RLIKE pat`.
// See https://dev.allegrosql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSharedSig) evalInt(event chunk.Event) (int64, bool, error) {
	expr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, true, err
	}

	re, err := b.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

type builtinRegexpSig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpSig(bf baseBuiltinFunc) *builtinRegexpSig {
	shared := builtinRegexpSharedSig{baseBuiltinFunc: bf}
	shared.compile = regexp.Compile
	return &builtinRegexpSig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpSig) Clone() builtinFunc {
	newSig := &builtinRegexpSig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

type builtinRegexpUTF8Sig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpUTF8Sig(bf baseBuiltinFunc) *builtinRegexpUTF8Sig {
	shared := builtinRegexpSharedSig{baseBuiltinFunc: bf}
	if defCauslate.IsCIDefCauslation(bf.defCauslation) {
		shared.compile = func(pat string) (*regexp.Regexp, error) {
			return regexp.Compile("(?i)" + pat)
		}
	} else {
		shared.compile = regexp.Compile
	}
	return &builtinRegexpUTF8Sig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRegexpUTF8Sig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

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

package expression

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/encrypt"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
	_ functionClass = &compressFunctionClass{}
	_ functionClass = &decodeFunctionClass{}
	_ functionClass = &desDecryptFunctionClass{}
	_ functionClass = &desEncryptFunctionClass{}
	_ functionClass = &encodeFunctionClass{}
	_ functionClass = &encryptFunctionClass{}
	_ functionClass = &md5FunctionClass{}
	_ functionClass = &oldPasswordFunctionClass{}
	_ functionClass = &passwordFunctionClass{}
	_ functionClass = &randomBytesFunctionClass{}
	_ functionClass = &sha1FunctionClass{}
	_ functionClass = &sha2FunctionClass{}
	_ functionClass = &uncompressFunctionClass{}
	_ functionClass = &uncompressedLengthFunctionClass{}
	_ functionClass = &validatePasswordStrengthFunctionClass{}
)

var (
	_ builtinFunc = &builtinAesDecryptSig{}
	_ builtinFunc = &builtinAesDecryptIVSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
	_ builtinFunc = &builtinAesEncryptIVSig{}
	_ builtinFunc = &builtinCompressSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
)

// aesModeAttr indicates that the key length and iv attribute for specific block_encryption_mode.
// keySize is the key length in bits and mode is the encryption mode.
// ivRequired indicates that initialization vector is required or not.
type aesModeAttr struct {
	modeName   string
	keySize    int
	ivRequired bool
}

var aesModes = map[string]*aesModeAttr{
	//TODO support more modes, permitted mode values are: ECB, CBC, CFB1, CFB8, CFB128, OFB
	"aes-128-ecb": {"ecb", 16, false},
	"aes-192-ecb": {"ecb", 24, false},
	"aes-256-ecb": {"ecb", 32, false},
	"aes-128-cbc": {"cbc", 16, true},
	"aes-192-cbc": {"cbc", 24, true},
	"aes-256-cbc": {"cbc", 32, true},
	"aes-128-ofb": {"ofb", 16, true},
	"aes-192-ofb": {"ofb", 24, true},
	"aes-256-ofb": {"ofb", 32, true},
	"aes-128-cfb": {"cfb", 16, true},
	"aes-192-cfb": {"cfb", 24, true},
	"aes-256-cfb": {"cfb", 32, true},
}

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = args[0].GetType().Flen // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode, _ := ctx.GetStochastikVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_decrypt")
		}
		sig := &builtinAesDecryptIVSig{bf, mode}
		sig.setPbCode(fidelpb.ScalarFuncSig_AesDecryptIV)
		return sig, nil
	}
	sig := &builtinAesDecryptSig{bf, mode}
	sig.setPbCode(fidelpb.ScalarFuncSig_AesDecrypt)
	return sig, nil
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) evalString(event chunk.Event) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.ctx.GetStochastikVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	key := encrypt.DeriveKeyMyALLEGROSQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "ecb":
		plainText, err = encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type builtinAesDecryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) evalString(event chunk.Event) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMyALLEGROSQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "cbc":
		plainText, err = encrypt.AESDecryptWithCBC([]byte(cryptStr), key, []byte(iv))
	case "ofb":
		plainText, err = encrypt.AESDecryptWithOFB([]byte(cryptStr), key, []byte(iv))
	case "cfb":
		plainText, err = encrypt.AESDecryptWithCFB([]byte(cryptStr), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = aes.BlockSize * (args[0].GetType().Flen/aes.BlockSize + 1) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode, _ := ctx.GetStochastikVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_encrypt")
		}
		sig := &builtinAesEncryptIVSig{bf, mode}
		sig.setPbCode(fidelpb.ScalarFuncSig_AesEncryptIV)
		return sig, nil
	}
	sig := &builtinAesEncryptSig{bf, mode}
	sig.setPbCode(fidelpb.ScalarFuncSig_AesEncrypt)
	return sig, nil
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) evalString(event chunk.Event) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.ctx.GetStochastikVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	key := encrypt.DeriveKeyMyALLEGROSQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "ecb":
		cipherText, err = encrypt.AESEncryptWithECB([]byte(str), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type builtinAesEncryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) evalString(event chunk.Event) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMyALLEGROSQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "cbc":
		cipherText, err = encrypt.AESEncryptWithCBC([]byte(str), key, []byte(iv))
	case "ofb":
		cipherText, err = encrypt.AESEncryptWithOFB([]byte(str), key, []byte(iv))
	case "cfb":
		cipherText, err = encrypt.AESEncryptWithCFB([]byte(str), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinDecodeSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Decode)
	return sig, nil
}

type builtinDecodeSig struct {
	baseBuiltinFunc
}

func (b *builtinDecodeSig) Clone() builtinFunc {
	newSig := &builtinDecodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals DECODE(str, password_str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) evalString(event chunk.Event) (string, bool, error) {
	dataStr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	decodeStr, err := encrypt.ALLEGROSQLDecode(dataStr, passwordStr)
	return decodeStr, false, err
}

type desDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *desDecryptFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_DECRYPT")
}

type desEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *desEncryptFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_ENCRYPT")
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.Flen = args[0].GetType().Flen
	sig := &builtinEncodeSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Encode)
	return sig, nil
}

type builtinEncodeSig struct {
	baseBuiltinFunc
}

func (b *builtinEncodeSig) Clone() builtinFunc {
	newSig := &builtinEncodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals ENCODE(crypt_str, password_str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) evalString(event chunk.Event) (string, bool, error) {
	decodeStr, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	dataStr, err := encrypt.ALLEGROSQLEncode(decodeStr, passwordStr)
	return dataStr, false, err
}

type encryptFunctionClass struct {
	baseFunctionClass
}

func (c *encryptFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "ENCRYPT")
}

type oldPasswordFunctionClass struct {
	baseFunctionClass
}

func (c *oldPasswordFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "OLD_PASSWORD")
}

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = allegrosql.PWDHashLen + 1
	sig := &builtinPasswordSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Password)
	return sig, nil
}

type builtinPasswordSig struct {
	baseBuiltinFunc
}

func (b *builtinPasswordSig) Clone() builtinFunc {
	newSig := &builtinPasswordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinPasswordSig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) evalString(event chunk.Event) (d string, isNull bool, err error) {
	pass, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", err != nil, err
	}

	if len(pass) == 0 {
		return "", false, nil
	}

	// We should append a warning here because function "PASSWORD" is deprecated since MyALLEGROSQL 5.7.6.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_password
	b.ctx.GetStochastikVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoRememristed.GenWithStackByArgs("PASSWORD"))

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1024 // Max allowed random bytes
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinRandomBytesSig{bf}
	return sig, nil
}

type builtinRandomBytesSig struct {
	baseBuiltinFunc
}

func (b *builtinRandomBytesSig) Clone() builtinFunc {
	newSig := &builtinRandomBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) evalString(event chunk.Event) (string, bool, error) {
	len, isNull, err := b.args[0].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if len < 1 || len > 1024 {
		return "", false, types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
	}
	buf := make([]byte, len)
	if n, err := rand.Read(buf); err != nil {
		return "", true, err
	} else if int64(n) != len {
		return "", false, errors.New("fail to generate random bytes")
	}
	return string(buf), false, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochastikVars().GetCharsetInfo()
	bf.tp.Flen = 32
	sig := &builtinMD5Sig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_MD5)
	return sig, nil
}

type builtinMD5Sig struct {
	baseBuiltinFunc
}

func (b *builtinMD5Sig) Clone() builtinFunc {
	newSig := &builtinMD5Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMD5Sig.
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) evalString(event chunk.Event) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	sum := md5.Sum([]byte(arg))
	hexStr := fmt.Sprintf("%x", sum)
	return hexStr, false, nil
}

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochastikVars().GetCharsetInfo()
	bf.tp.Flen = 40
	sig := &builtinSHA1Sig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_SHA1)
	return sig, nil
}

type builtinSHA1Sig struct {
	baseBuiltinFunc
}

func (b *builtinSHA1Sig) Clone() builtinFunc {
	newSig := &builtinSHA1Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SHA1(str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) evalString(event chunk.Event) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := sha1.New()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.DefCauslate = ctx.GetStochastikVars().GetCharsetInfo()
	bf.tp.Flen = 128 // sha512
	sig := &builtinSHA2Sig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_SHA2)
	return sig, nil
}

type builtinSHA2Sig struct {
	baseBuiltinFunc
}

func (b *builtinSHA2Sig) Clone() builtinFunc {
	newSig := &builtinSHA2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// Supported hash length of SHA-2 family
const (
	SHA0   = 0
	SHA224 = 224
	SHA256 = 256
	SHA384 = 384
	SHA512 = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) evalString(event chunk.Event) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	hashLength, isNull, err := b.args[1].EvalInt(b.ctx, event)
	if isNull || err != nil {
		return "", isNull, err
	}
	var hasher hash.Hash
	switch int(hashLength) {
	case SHA0, SHA256:
		hasher = sha256.New()
	case SHA224:
		hasher = sha256.New224()
	case SHA384:
		hasher = sha512.New384()
	case SHA512:
		hasher = sha512.New()
	}
	if hasher == nil {
		return "", true, nil
	}

	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	if _, err = io.INTERLOCKy(&out, r); err != nil {
		return nil, err
	}
	err = r.Close()
	return out.Bytes(), err
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	srcLen := args[0].GetType().Flen
	compressBound := srcLen + (srcLen >> 12) + (srcLen >> 14) + (srcLen >> 25) + 13
	if compressBound > allegrosql.MaxBlobWidth {
		compressBound = allegrosql.MaxBlobWidth
	}
	bf.tp.Flen = compressBound
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinCompressSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Compress)
	return sig, nil
}

type builtinCompressSig struct {
	baseBuiltinFunc
}

func (b *builtinCompressSig) Clone() builtinFunc {
	newSig := &builtinCompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals COMPRESS(str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) evalString(event chunk.Event) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}

	// According to doc: Empty strings are stored as empty strings.
	if len(str) == 0 {
		return "", false, nil
	}

	compressed, err := deflate([]byte(str))
	if err != nil {
		return "", true, nil
	}

	resultLength := 4 + len(compressed)

	// append "." if ends with space
	shouldAppendSuffix := compressed[len(compressed)-1] == 32
	if shouldAppendSuffix {
		resultLength++
	}

	buffer := make([]byte, resultLength)
	binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
	INTERLOCKy(buffer[4:], compressed)

	if shouldAppendSuffix {
		buffer[len(buffer)-1] = '.'
	}

	return string(buffer), false, nil
}

type uncompressFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = allegrosql.MaxBlobWidth
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUncompressSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_Uncompress)
	return sig, nil
}

type builtinUncompressSig struct {
	baseBuiltinFunc
}

func (b *builtinUncompressSig) Clone() builtinFunc {
	newSig := &builtinUncompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) evalString(event chunk.Event) (string, bool, error) {
	sc := b.ctx.GetStochastikVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return "", true, err
	}
	if len(payload) == 0 {
		return "", false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	if length < uint32(len(bytes)) {
		sc.AppendWarning(errZlibZBuf)
		return "", true, nil
	}
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 10
	sig := &builtinUncompressedLengthSig{bf}
	sig.setPbCode(fidelpb.ScalarFuncSig_UncompressedLength)
	return sig, nil
}

type builtinUncompressedLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinUncompressedLengthSig) Clone() builtinFunc {
	newSig := &builtinUncompressedLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.allegrosql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(event chunk.Event) (int64, bool, error) {
	sc := b.ctx.GetStochastikVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(b.ctx, event)
	if isNull || err != nil {
		return 0, true, err
	}
	if len(payload) == 0 {
		return 0, false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	len := binary.LittleEndian.Uint32([]byte(payload)[0:4])
	return int64(len), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(ctx stochastikctx.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "VALIDATE_PASSWORD_STRENGTH")
}

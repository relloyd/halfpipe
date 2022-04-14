package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
)

var (
	fileEncrKey = []byte("vp;8vJbo$7aPXD^34zxY(LUo]d4EodCP")
)

// EncryptedFile is a simple struct able to split file paths into the components to improve readability of code.
type EncryptedFile struct {
	Dirname     string
	FileName    string
	FilePrefix  string
	FileExt     string
	FullPath    string
	mu          sync.Mutex
	fileCreated bool
}

func NewEncryptedFileInConfigHomeDir(filename string) *EncryptedFile {
	dirName := mustGetConfigHomeDir()
	f := &EncryptedFile{Dirname: dirName, FileName: filename}
	f.FullPath = path.Join(dirName, filename)
	f.FileExt = strings.TrimLeft(path.Ext(filename), ".")
	f.FilePrefix = strings.TrimRight(f.FileName, "."+f.FileExt)
	return f
}

func (f *EncryptedFile) Set(text []byte) (err error) {
	// Generate a new aes cipher using our 32 byte long key
	c, err := aes.NewCipher(fileEncrKey)
	if err != nil {
		return err
	}
	// Use GCM or Galois/Counter Mode, is a mode of operation for symmetric key cryptographic block ciphers.
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return err
	}
	// Create a new byte array the size of the nonce which must be passed to Seal.
	// And populate our nonce with a cryptographically secure random sequence.
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	// Encrypt our text using the Seal function.
	// Seal encrypts and authenticates plaintext, authenticates the
	// additional data and appends the result to dst, returning the updated
	// slice. The nonce must be NonceSize() bytes long and unique for all
	// time, for a given key.
	sealedBytes := gcm.Seal(nonce, nonce, text, nil)
	// Encode to b64.
	b64 := base64.StdEncoding.EncodeToString(sealedBytes)
	// Create the config file if required.
	if !fileExists(f.FullPath) { // if the file does not exist...
		if err := makeDir(f.Dirname); err != nil { // if we could not create the config directory...
			return err
		}
	}
	err = ioutil.WriteFile(f.FullPath, []byte(b64), 0600)
	if err != nil {
		return err
	}
	return nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func (f *EncryptedFile) Get() (text []byte, err error) {
	if !fileExists(f.FullPath) { // if the file does not exist...
		return nil, FileNotFoundError{f.FullPath}
	}
	// Read b64 file contents.
	b64, err := ioutil.ReadFile(f.FullPath)
	if err != nil {
		return nil, err
	}
	cipherText, err := base64.StdEncoding.DecodeString(string(b64))
	if err != nil {
		return nil, err
	}
	return Decrypt(cipherText, fileEncrKey)
}

func Decrypt(text []byte, key []byte) ([]byte, error) {
	// Decrypt bytes.
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(text) < nonceSize {
		return nil, fmt.Errorf("encrypted text is too short")
	}
	nonce, cipherText := text[:nonceSize], text[nonceSize:]
	b, err := gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}
	// Return plaintext bytes.
	return b, nil
}

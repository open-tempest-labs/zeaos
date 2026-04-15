package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/scrypt"
	"golang.org/x/term"
)

// CredentialField is a single named value within a credential set.
type CredentialField struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Credential is a named collection of key/value fields.
type Credential struct {
	Name   string            `json:"name"`
	Fields []CredentialField `json:"fields"`
}

// Get returns the value for the given key, or an error if not found.
func (c *Credential) Get(key string) (string, error) {
	for _, f := range c.Fields {
		if f.Key == key {
			return f.Value, nil
		}
	}
	return "", fmt.Errorf("credential %q has no field %q", c.Name, key)
}

// encryptedFile is the on-disk representation of an encrypted credential.
type encryptedFile struct {
	V     int    `json:"v"`
	Nonce string `json:"nonce"` // base64 12-byte AES-GCM nonce
	Data  string `json:"data"`  // base64 AES-GCM ciphertext
}

// CredStore manages encrypted credentials stored in a directory.
// The encryption key is derived from a user password via scrypt and cached
// in memory for the lifetime of the session.
type CredStore struct {
	dir string // e.g. ~/.zeaos/credentials
	key []byte // 32-byte AES-256 key, nil = locked
}

// NewCredStore returns a CredStore rooted at dir. The directory and a random
// salt file are created if they do not exist. The store is initially locked;
// call Unlock before reading or writing credentials.
func NewCredStore(dir string) (*CredStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("credentials: cannot create store dir: %w", err)
	}
	return &CredStore{dir: dir}, nil
}

// IsUnlocked reports whether the store has a cached encryption key.
func (cs *CredStore) IsUnlocked() bool { return cs.key != nil }

// Dir returns the directory where credentials and the key file are stored.
func (cs *CredStore) Dir() string { return cs.dir }

// Unlock derives the AES-256 key from password and the stored salt, caching
// it for the session. If no salt file exists a fresh one is created.
func (cs *CredStore) Unlock(password string) error {
	salt, err := cs.loadOrCreateSalt()
	if err != nil {
		return err
	}
	// scrypt parameters: N=32768, r=8, p=1, keyLen=32
	key, err := scrypt.Key([]byte(password), salt, 32768, 8, 1, 32)
	if err != nil {
		return fmt.Errorf("credentials: key derivation failed: %w", err)
	}
	cs.key = key
	return nil
}

// UnlockInteractive unlocks the store, trying each method in order:
//  1. Already unlocked — no-op.
//  2. ~/.zeaos/credentials/.key file (chmod 600) — silent, supports unattended
//     scheduled runs where no human is present to type a password.
//  3. Interactive password prompt (no echo) — fallback for interactive sessions.
func (cs *CredStore) UnlockInteractive() error {
	if cs.IsUnlocked() {
		return nil
	}
	// Try .key file first so unattended/scheduled zearun calls work silently.
	if err := cs.unlockFromKeyFile(); err == nil {
		return nil
	}
	// Fall back to interactive prompt.
	fmt.Print("Credentials password: ")
	raw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return fmt.Errorf("credentials: could not read password: %w", err)
	}
	return cs.Unlock(string(raw))
}

// unlockFromKeyFile attempts to read the password from ~/.zeaos/credentials/.key
// (chmod 600). Returns an error if the file does not exist or decryption fails.
func (cs *CredStore) unlockFromKeyFile() error {
	keyPath := filepath.Join(cs.dir, ".key")
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return err // file absent — not an error worth logging
	}
	password := strings.TrimSpace(string(data))
	if password == "" {
		return fmt.Errorf("credentials: .key file is empty")
	}
	return cs.Unlock(password)
}

// Save encrypts and writes a credential to disk.
func (cs *CredStore) Save(cred Credential) error {
	if err := cs.requireUnlocked(); err != nil {
		return err
	}
	plain, err := json.Marshal(cred)
	if err != nil {
		return err
	}
	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	ciphertext := gcm.Seal(nil, nonce, plain, nil)

	ef := encryptedFile{
		V:     1,
		Nonce: base64.StdEncoding.EncodeToString(nonce),
		Data:  base64.StdEncoding.EncodeToString(ciphertext),
	}
	out, err := json.MarshalIndent(ef, "", "  ")
	if err != nil {
		return err
	}
	path := cs.credPath(cred.Name)
	if err := os.WriteFile(path, out, 0600); err != nil {
		return fmt.Errorf("credentials: write %s: %w", path, err)
	}
	return nil
}

// Load decrypts and returns the named credential.
func (cs *CredStore) Load(name string) (Credential, error) {
	if err := cs.requireUnlocked(); err != nil {
		return Credential{}, err
	}
	raw, err := os.ReadFile(cs.credPath(name))
	if err != nil {
		if os.IsNotExist(err) {
			return Credential{}, fmt.Errorf("credentials: %q not found", name)
		}
		return Credential{}, fmt.Errorf("credentials: read %s: %w", name, err)
	}
	var ef encryptedFile
	if err := json.Unmarshal(raw, &ef); err != nil {
		return Credential{}, fmt.Errorf("credentials: malformed file for %q", name)
	}
	nonce, err := base64.StdEncoding.DecodeString(ef.Nonce)
	if err != nil {
		return Credential{}, fmt.Errorf("credentials: bad nonce for %q", name)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(ef.Data)
	if err != nil {
		return Credential{}, fmt.Errorf("credentials: bad data for %q", name)
	}
	block, err := aes.NewCipher(cs.key)
	if err != nil {
		return Credential{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return Credential{}, err
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return Credential{}, fmt.Errorf("credentials: decryption failed for %q — wrong password?", name)
	}
	var cred Credential
	if err := json.Unmarshal(plain, &cred); err != nil {
		return Credential{}, fmt.Errorf("credentials: corrupt data for %q", name)
	}
	return cred, nil
}

// List returns the names of all stored credentials.
func (cs *CredStore) List() ([]string, error) {
	entries, err := os.ReadDir(cs.dir)
	if err != nil {
		return nil, fmt.Errorf("credentials: cannot list store: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".cred") {
			names = append(names, strings.TrimSuffix(e.Name(), ".cred"))
		}
	}
	return names, nil
}

// Delete removes the named credential file.
func (cs *CredStore) Delete(name string) error {
	path := cs.credPath(name)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("credentials: %q not found", name)
		}
		return fmt.Errorf("credentials: delete %s: %w", name, err)
	}
	return nil
}

// GetField loads the named credential and returns the value for key.
// Auto-prompts for the password if the store is locked.
func (cs *CredStore) GetField(name, key string) (string, error) {
	if err := cs.UnlockInteractive(); err != nil {
		return "", err
	}
	cred, err := cs.Load(name)
	if err != nil {
		return "", err
	}
	return cred.Get(key)
}

func (cs *CredStore) credPath(name string) string {
	return filepath.Join(cs.dir, name+".cred")
}

func (cs *CredStore) requireUnlocked() error {
	if !cs.IsUnlocked() {
		return fmt.Errorf("credentials: store is locked — run 'credentials unlock'")
	}
	return nil
}

func (cs *CredStore) loadOrCreateSalt() ([]byte, error) {
	saltPath := filepath.Join(cs.dir, ".salt")
	raw, err := os.ReadFile(saltPath)
	if err == nil {
		salt, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err == nil {
			return salt, nil
		}
	}
	// Create a new random salt.
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("credentials: cannot generate salt: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(salt)
	if err := os.WriteFile(saltPath, []byte(encoded+"\n"), 0600); err != nil {
		return nil, fmt.Errorf("credentials: cannot write salt: %w", err)
	}
	return salt, nil
}

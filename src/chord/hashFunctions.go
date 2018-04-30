package main

import (
	"crypto/sha1"
	"math/big"
	"crypto/rand"
)

func hash_0(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    return new(big.Int).SetBytes(hasher.Sum(nil)) 
}

func hash_1(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    randomBytes := []byte{50, 197, 57, 151, 232, 49, 93, 129, 132, 198, 251, 191, 151, 235, 237, 227, 156, 57, 45, 10}
    result := new(big.Int).Xor(new(big.Int).SetBytes(hasher.Sum(nil)) , new(big.Int).SetBytes(randomBytes))
    return new(big.Int).Mod(result, hashMod)
}

func hash_2(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    randomBytes := []byte{215, 232, 241, 176, 226, 235, 188, 95, 147, 25, 107, 167, 231, 147, 239, 128, 226, 196, 219, 55}
    result := new(big.Int).Xor(new(big.Int).SetBytes(hasher.Sum(nil)) , new(big.Int).SetBytes(randomBytes))
    return new(big.Int).Mod(result, hashMod)
}

func hash_3(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    randomBytes := []byte{38, 195, 138, 53, 14, 209, 7, 26, 30, 142, 162, 88, 131, 204, 193, 123, 247, 193, 186, 215}
    result := new(big.Int).Xor(new(big.Int).SetBytes(hasher.Sum(nil)) , new(big.Int).SetBytes(randomBytes))
    return new(big.Int).Mod(result, hashMod)
}

func GenerateRandomBytes(n int) ([]byte, error) {
    b := make([]byte, n)
    _, err := rand.Read(b)
    if err != nil {
        return nil, err
    }
    return b, nil
}

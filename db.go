package main

// DB is the in-memory store. No mutex needed — only ever touched
// by the single event loop goroutine.
type DB struct {
	data map[string]string
}

func NewDB() *DB {
	return &DB{data: make(map[string]string)}
}

func (db *DB) Set(key, value string) {
	db.data[key] = value
}

func (db *DB) Get(key string) (string, bool) {
	v, ok := db.data[key]
	return v, ok
}

func (db *DB) Del(key string) bool {
	_, ok := db.data[key]
	delete(db.data, key)
	return ok
}

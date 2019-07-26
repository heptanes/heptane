package heptane

// CacheKey is the key of a cache entry.
type CacheKey string

// CacheValue is the value of a cache entry. A nil slice means a cache miss, a
// non nil slice means a cache hit even if it is empty.
type CacheValue []byte

// CacheAccess is the interface of all types that represent an access to a
// cache.
type CacheAccess interface{}

// CacheGet specifies the retrieval of a cache entry.
type CacheGet struct {
	// CacheKey is the key of the cache entry.
	Key CacheKey
	// CacheVlaue will contain the value of the cache entry.
	Value CacheValue
}

// CacheSet specifies the creation or update of a cache entry.
type CacheSet struct {
	// CacheKey is the key of the cache entry.
	Key CacheKey
	// CacheVlaue is the value of the cache entry.
	Value CacheValue
}

// CacheProvider is the interface of all implementations that access caches
// directly.
type CacheProvider interface {
	// Access performs the given acccess to the cache.
	Access(CacheAccess) error
	// AccessSlice performs several acccesses to the cache.
	AccessSlice([]CacheAccess) []error
}

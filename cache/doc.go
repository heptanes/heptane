/*
Interface definition of CacheProvider.

CacheProviders are simple key value pairs that need to provide only the
operations Set and Get. Values are stored without expiration time.

CacheGet must be passed as reference so the Value set by the CacheProvider may
be read by the client code.
*/
package heptane

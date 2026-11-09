// Package shard assigns items to shards by a hash of their internal ID
// (SPEC §4) and searches the item vectors split that way (D028).
package shard

// Of returns the shard, in [0, n), of internal ID id: SplitMix64's
// finalizer over the ID, modulo n, so that runs of IDs spread evenly. The
// mapping decides where every item lives and must not change.
func Of(id int32, n int) int {
	x := uint64(uint32(id))
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return int(x % uint64(n))
}

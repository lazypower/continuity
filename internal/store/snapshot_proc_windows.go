//go:build windows

package store

// processAlive on windows conservatively reports true: without a cheap
// liveness probe we fail closed and assume a serve may be holding the DB.
// Windows is not a primary target for this feature in v1.
func processAlive(pid int) bool {
	return true
}

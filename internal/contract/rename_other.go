//go:build !darwin

package contract

import "errors"

func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	return errors.New("exclusive publication is supported only on the macOS target")
}

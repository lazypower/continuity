//go:build windows

package cli

import "fmt"

func platformServiceStatus() (bool, string) {
	return false, ""
}

func platformServicePlan() (string, error) {
	return "", fmt.Errorf("install-service is not supported on Windows")
}

func platformServiceInstall() (string, error) {
	return "", fmt.Errorf("install-service is not supported on Windows")
}

func platformUninstallPlan() (string, error) {
	return "", fmt.Errorf("uninstall-service is not supported on Windows")
}

func platformServiceRemove() error {
	return fmt.Errorf("uninstall-service is not supported on Windows")
}

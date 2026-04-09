package zeaberg

import (
	"io"
	"os"
	"strings"
)

func joinStrings(ss []string, sep string) string {
	return strings.Join(ss, sep)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

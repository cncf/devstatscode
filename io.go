package devstatscode

import (
	"io/ioutil"
	"strings"
)

// ReadFile tries to read any filename, but have a fallback
// it attempts to replace current project name with shared: /proj/ -> /shared/
// This is to allow reading files that can be shared between projects
func ReadFile(ctx *Ctx, path string) ([]byte, error) {
	data, err := ioutil.ReadFile(path)
	if err == nil || ctx.Project == "" {
		if ctx.Debug > 0 {
			Printf("lib.ReadFile('%s'): ok\n", path)
		}
		return data, err
	}
	path = strings.Replace(path, "/"+ctx.Project+"/", "/shared/", -1)
	data, err = ioutil.ReadFile(path)
	if err == nil && ctx.Debug > 0 {
		Printf("lib.ReadFile('%s'): ok", path)
	}
	if err != nil {
		Printf("lib.ReadFile('%s'): error: %+v", path, err)
	}
	return data, err
}

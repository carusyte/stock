package util

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/ssgreg/repeat"
)

//CPUUsage returns current cpu busy percentage.
func CPUUsage() (idle float64, e error) {
	var ps []float64
	ps, e = cpu.Percent(0, false)
	if e != nil {
		return
	}
	return ps[0], e
}

//ParseLines parses target file specified by absolute path.
func ParseLines(path string, retry int, parser func(no int, line []byte) error, init func()) (e error) {
	op := func(c int) error {
		var f *os.File
		if f, e = os.Open(path); e != nil {
			log.Printf("#%d failed to open file %s: %+v", c, path, e)
			return repeat.HintTemporary(e)
		}
		defer f.Close()
		rd := bufio.NewReader(f)
		var line []byte
		no := 1
		init()
		for {
			ln, ispr, e := rd.ReadLine()
			if e != nil {
				if e == io.EOF {
					return nil
				}
				log.Printf("failed to read line: %+v", e)
				return repeat.HintTemporary(e)
			}
			line = append(line, ln...)
			if !ispr {
				if e := parser(no, line); e != nil {
					log.Printf("failed to parse line@%d : %+v", no, e)
					return repeat.HintStop(e)
				}
				no++
				line = make([]byte, 0, 128)
			}
		}
	}
	if retry > 0 {
		e = repeat.Repeat(
			repeat.FnWithCounter(op),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(retry),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
			),
		)
	} else {
		e = op(0)
	}
	return
}

//MkDirAll similar to os.MkDirAll, but with retry when failed.
func MkDirAll(path string, perm os.FileMode) (e error) {
	op := func(c int) error {
		if e = os.MkdirAll(path, perm); e != nil {
			return repeat.HintTemporary(e)
		}
		if e = os.Chmod(path, perm); e != nil {
			return repeat.HintTemporary(e)
		}
		return nil
	}
	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)
	return
}

//FileExists checks (with optional retry) whether the specified file exists
//in the provided directory (or optionally its sub-directory).
func FileExists(dir, name string, searchSubDirectory, retry bool) (exists bool, path string, e error) {
	paths := []string{filepath.Join(dir, name)}
	op := func(c int) error {
		if searchSubDirectory {
			dirs, err := ioutil.ReadDir(dir)
			if err != nil {
				log.Printf("#%d failed to read content from %s: %+v", c, dir, err)
				return repeat.HintTemporary(errors.WithStack(err))
			}
			for _, d := range dirs {
				if d.IsDir() {
					paths = append(paths, filepath.Join(dir, d.Name(), name))
				}
			}
		}
		for _, p := range paths {
			_, e = os.Stat(p)
			if e != nil {
				if !os.IsNotExist(e) {
					log.Printf("#%d failed to check existence of %s : %+v", c, p, e)
					return repeat.HintTemporary(errors.WithStack(e))
				} else {
					e = nil
				}
			} else {
				exists = true
				path = p
				return nil
			}
		}
		return nil
	}
	if retry {
		e = repeat.Repeat(
			repeat.FnWithCounter(op),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(conf.Args.DefaultRetry),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
			),
		)
	} else {
		e = op(0)
	}

	return
}

//NumOfFiles counts files matching the provided pattern
//under the specified directory (or optionally its sub-directory).
func NumOfFiles(dir, pattern string, searchSubDirectory bool) (num int, e error) {
	op := func(c int) error {
		num = 0
		if _, e = os.Stat(dir); e != nil {
			if os.IsNotExist(e) {
				return repeat.HintStop(e)
			} else {
				log.Printf("#%d failed to read stat for %s: %+v", c, dir, e)
				return repeat.HintTemporary(e)
			}
		}
		paths := []string{dir}
		if searchSubDirectory {
			dirs, e := ioutil.ReadDir(dir)
			if e != nil {
				log.Printf("#%d failed to read content from %s: %+v", c, dir, e)
				return repeat.HintTemporary(e)
			}
			for _, d := range dirs {
				if d.IsDir() {
					paths = append(paths, filepath.Join(dir, d.Name()))
				}
			}
		}
		for _, p := range paths {
			files, e := ioutil.ReadDir(p)
			if e != nil {
				log.Printf("#%d failed to read content from %s: %+v", c, p, e)
				return repeat.HintTemporary(e)
			}
			for _, f := range files {
				if !f.IsDir() {
					m, e := regexp.MatchString(pattern, f.Name())
					if e != nil {
						return repeat.HintStop(e)
					}
					if m {
						num++
					}
				}
			}
		}
		return nil
	}

	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)

	return
}

//WriteJSONFile writes provided payload object pointer as (gzipped) json formatted file.
//it first tries to write to a *.tmp file, then renames it to *.json(.gz),
//then returns the final path of the written file. If the final file already exists, error is returned.
//path parameter should not include file extensions.
func WriteJSONFile(payload interface{}, path string, compress bool) (finalPath string, e error) {
	op := func(c int) error {
		if c > 0 {
			log.Printf("#%d retrying to write json file to %s...", c, path)
		}
		tmp := fmt.Sprintf("%s.tmp", path)
		if compress {
			finalPath = fmt.Sprintf("%s.json.gz", path)
		} else {
			finalPath = fmt.Sprintf("%s.json", path)
		}
		dir, name := filepath.Dir(tmp), filepath.Base(tmp)
		ex, _, e := FileExists(dir, name, false, false)
		if e != nil {
			return repeat.HintStop(errors.WithMessage(e, "unable to check existence for "+tmp))
		}
		if ex {
			os.Remove(tmp)
		}
		dir, name = filepath.Dir(finalPath), filepath.Base(finalPath)
		ex, _, e = FileExists(dir, name, false, false)
		if e != nil {
			return repeat.HintStop(errors.WithMessage(e, "unable to check existence for "+finalPath))
		}
		if ex {
			return repeat.HintStop(fmt.Errorf("%s already exists", finalPath))
		}
		jsonBytes, e := json.Marshal(payload)
		if e != nil {
			log.Printf("#%d failed to marshal payload %+v: %+v", c, payload, e)
			return repeat.HintStop(e)
		}
		_, e = bufferedWrite(tmp, jsonBytes, compress)
		if e != nil {
			log.Printf("#%d %+v", c, e)
			return repeat.HintTemporary(e)
		}
		e = os.Rename(tmp, finalPath)
		if e != nil {
			log.Printf("#%d failed to rename %s to %s: %+v", c, tmp, finalPath, e)
			return repeat.HintTemporary(e)
		}
		return nil
	}

	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(15*time.Second).Set(),
		),
	)

	return
}

func bufferedWrite(path string, data []byte, compress bool) (nn int, e error) {
	var wt io.Writer
	wt, e = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if e != nil {
		return nn, errors.WithStack(
			errors.WithMessage(e, fmt.Sprintf("failed to create file %s", path)))
	}
	if compress {
		wt, e = gzip.NewWriterLevel(wt, flate.BestCompression)
		if e != nil {
			return nn, errors.WithStack(
				errors.WithMessage(e, fmt.Sprintf("failed to create gzip writer %s", path)))
		}
	}
	bw := bufio.NewWriter(wt)
	defer func() {
		bw.Flush()
		wt.(io.Closer).Close()
	}()
	nn, e = bw.Write(data)
	if e != nil {
		os.Remove(path)
		return nn, errors.WithStack(
			errors.WithMessage(e, fmt.Sprintf("failed to write bytes to %s", path)))
	}
	return
}

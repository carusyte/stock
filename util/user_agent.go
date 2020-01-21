package util

import (
	"archive/tar"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/carusyte/roprox/data"
	"github.com/carusyte/roprox/types"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/ssgreg/repeat"
)

var (
	agentPool []string
	uaLock    = sync.RWMutex{}
)

//PickUserAgent picks a user agent string from the pool randomly.
//if the pool is not populated, it will trigger the initialization process
//to fetch user agent lists from remote server.
func PickUserAgent() (ua string, e error) {
	uaLock.Lock()
	defer uaLock.Unlock()

	if len(agentPool) > 0 {
		return agentPool[rand.Intn(len(agentPool))], nil
	}
	//first, load from database
	agents := loadUserAgents()
	refresh := false
	if len(agents) != 0 {
		var latest time.Time
		latest, e = time.Parse(global.DateTimeFormat, agents[0].UpdatedAt)
		if e != nil {
			return
		}
		if time.Now().Sub(latest).Hours() >=
			float64(time.Duration(conf.Args.Network.UserAgentLifespan*24)*time.Hour) {
			refresh = true
		}
	}
	//if none, or outdated, refresh table from remote server
	if refresh || len(agents) == 0 {
		//download sample file and load into database server
		log.Info("fetching user agent list from remote server...")
		exePath, e := os.Executable()
		if e != nil {
			log.Panicln("failed to get executable path", e)
		}
		path, e := filepath.EvalSymlinks(exePath)
		if e != nil {
			log.Panicln("failed to evaluate symlinks, ", exePath, e)
		}
		local := filepath.Join(filepath.Dir(path), filepath.Base(conf.Args.Network.UserAgents))
		if _, e := os.Stat(local); e == nil {
			os.Remove(local)
		}
		e = downloadFile(local, conf.Args.Network.UserAgents)
		defer os.Remove(local)
		if e != nil {
			log.Panicln("failed to download user agent sample file ", conf.Args.Network.UserAgents, e)
		}
		agents, e = readCSV(local)
		if e != nil {
			log.Panicln("failed to download and read csv, ", local, e)
		}
		mergeAgents(agents)
		log.Infof("successfully fetched %d user agents from remote server.", len(agentPool))
		//reload agents from database
		agents = loadUserAgents()
	}
	for _, a := range agents {
		agentPool = append(agentPool, a.UserAgent)
	}
	return agentPool[rand.Intn(len(agentPool))], nil
}

func loadUserAgents() (agents []*types.UserAgent) {
	_, e := data.DB.Select(&agents, "select * from user_agents where hardware_type = ? order by updated_at desc", "computer")
	if e != nil {
		if "sql: no rows in result set" != e.Error() {
			log.Panicln("failed to run sql", e)
		}
	}
	return
}

func mergeAgents(agents []*types.UserAgent) (e error) {
	fields := []string{
		"id", "user_agent", "times_seen", "simple_software_string", "software_name", "software_version", "software_type",
		"software_sub_type", "hardware_type", "first_seen_at", "last_seen_at", "updated_at",
	}
	numFields := len(fields)
	holders := make([]string, numFields)
	for i := range holders {
		holders[i] = "?"
	}
	holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
	valueStrings := make([]string, 0, len(agents))
	valueArgs := make([]interface{}, 0, len(agents)*numFields)
	for _, a := range agents {
		valueStrings = append(valueStrings, holderString)
		valueArgs = append(valueArgs, a.ID)
		valueArgs = append(valueArgs, a.UserAgent)
		valueArgs = append(valueArgs, a.TimesSeen)
		valueArgs = append(valueArgs, a.SimpleSoftwareString)
		valueArgs = append(valueArgs, a.SoftwareName)
		valueArgs = append(valueArgs, a.SoftwareVersion)
		valueArgs = append(valueArgs, a.SoftwareType)
		valueArgs = append(valueArgs, a.SoftwareSubType)
		valueArgs = append(valueArgs, a.HardWareType)
		valueArgs = append(valueArgs, a.FirstSeenAt)
		valueArgs = append(valueArgs, a.LastSeenAt)
		valueArgs = append(valueArgs, a.UpdatedAt)
	}

	var updFieldStr []string
	for _, f := range fields {
		if "id" == f {
			continue
		}
		updFieldStr = append(updFieldStr, fmt.Sprintf("%[1]s=values(%[1]s)", f))
	}

	retry := 5
	rt := 0
	stmt := fmt.Sprintf("INSERT INTO user_agents (%s) VALUES %s on duplicate key update %s",
		strings.Join(fields, ","), strings.Join(valueStrings, ","), strings.Join(updFieldStr, ","))
	for ; rt < retry; rt++ {
		_, e = data.DB.Exec(stmt, valueArgs...)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				log.Panicln("failed to merge user_agent", e)
			}
		}
		return nil
	}
	log.Panicln("failed to merge user_agent", e)
	return
}

func readCSV(src string) (agents []*types.UserAgent, err error) {
	f, err := os.Open(src)
	if err != nil {
		return
	}
	defer f.Close()

	gzf, err := gzip.NewReader(f)
	if err != nil {
		return
	}

	tarReader := tar.NewReader(gzf)

	for {
		var header *tar.Header
		header, err = tarReader.Next()
		if err == io.EOF {
			return
		} else if err != nil {
			return
		}

		name := header.Name

		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			if !strings.EqualFold(".csv", filepath.Ext(name)) {
				continue
			}
		default:
			continue
		}

		csvReader := csv.NewReader(tarReader)
		var lines [][]string
		lines, err = csvReader.ReadAll()
		if err != nil {
			return
		}

		for i, ln := range lines {
			if i == 0 {
				//skip header line
				continue
			}
			agents = append(agents, &types.UserAgent{
				ID:                   ln[0],
				UserAgent:            ln[1],
				TimesSeen:            ln[2],
				SimpleSoftwareString: ln[3],
				SoftwareName:         ln[7],
				SoftwareVersion:      ln[10],
				SoftwareType:         ln[22],
				SoftwareSubType:      ln[23],
				HardWareType:         ln[25],
				FirstSeenAt:          ln[35],
				LastSeenAt:           ln[36],
				UpdatedAt:            time.Now().Format(global.DateTimeFormat),
			})
		}
		break
	}
	return
}

func downloadFile(filepath string, url string) (err error) {

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	var resp *http.Response
	op := func(c int) error {
		resp, err = http.Get(url)
		return repeat.HintTemporary(err)
	}
	err = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

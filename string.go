package devstatscode

import (
	"bytes"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// PrepareQuickRangeQuery Perpares query using either ready `period` string or using `from` and `to` strings
// Values to replace are specially encoded {{period:alias.column}}
// Can either replace with: (alias.column >= now() - 'period'::interval)
// Or (alias.column >= 'from' and alias.column < 'to')
func PrepareQuickRangeQuery(sql, period, from, to string) (string, string) {
	start := 0
	startPatt := "{{period:"
	startPattLen := len(startPatt)
	endPatt := "}}"
	endPattLen := len(endPatt)
	lenSQL := len(sql)
	res := ""
	sHours := "0"
	periodMode := false
	if period != "" {
		periodMode = true
		sHours = IntervalHours(period)
	} else {
		if from != "" && to != "" {
			tFrom := TimeParseAny(from)
			tTo := TimeParseAny(to)
			from = ToYMDHMSDate(tFrom)
			to = ToYMDHMSDate(tTo)
			sHours = RangeHours(tFrom, tTo)
		}
	}
	for {
		idx1 := strings.Index(sql[start:], startPatt)
		if idx1 == -1 {
			break
		}
		idx2 := strings.Index(sql[start+idx1:], endPatt)
		col := sql[start+idx1+startPattLen : start+idx1+idx2]
		res += sql[start : start+idx1]
		if periodMode {
			res += " (" + col + " >= now() - '" + period + "'::interval) "
		} else {
			if from == "" || to == "" {
				return "You need to provide either non-empty `period` or non empty `from` and `to`", sHours
			}
			res += " (" + col + " >= '" + from + "' and " + col + " < '" + to + "') "
		}
		start += idx1 + idx2 + endPattLen
	}
	res += sql[start:lenSQL]
	if periodMode {
		res = strings.Replace(res, "{{from}}", "(now() -'"+period+"'::interval)", -1)
		res = strings.Replace(res, "{{to}}", "(now())", -1)
	} else {
		res = strings.Replace(res, "{{from}}", "'"+from+"'", -1)
		res = strings.Replace(res, "{{to}}", "'"+to+"'", -1)
	}
	return res, sHours
}

// SafeUTF8String - make sure string is UTF-8 valid
func SafeUTF8String(input string) string {
	return string(bytes.ToValidUTF8([]byte(strings.Replace(input, "\x00", "", -1)), []byte("")))
}

// Slugify replace all whitespace with "-", remove all non-word letters downcase
func Slugify(arg string) string {
	re := regexp.MustCompile(`[^\w-]+`)
	arg = re.ReplaceAllLiteralString(arg, "-")
	return strings.ToLower(arg)
}

// GetHidden - return list of shas to replace
func GetHidden(ctx *Ctx, configFile string) (shaMap map[string]string) {
	shaMap = make(map[string]string)
	f, err := os.Open(configFile)
	if err != nil {
		f, err = os.Open(ctx.DataDir + "/" + configFile)
	}
	if err == nil {
		defer func() { _ = f.Close() }()
		reader := csv.NewReader(f)
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				FatalOnError(err)
			}
			sha := row[0]
			if sha == "sha1" {
				continue
			}
			shaMap[sha] = "anon-" + sha
		}
	}
	return
}

// MaybeHideFunc - use closure as a data storage
func MaybeHideFunc(shas map[string]string) (f func(string) string) {
	cache := make(map[string]string)
	f = func(arg string) string {
		var sha string
		sha, ok := cache[arg]
		if !ok {
			hash := sha1.New()
			_, err := hash.Write([]byte(arg))
			FatalOnError(err)
			sha = hex.EncodeToString(hash.Sum(nil))
			cache[arg] = sha
		}
		anon, ok := shas[sha]
		if ok {
			return anon
		}
		return arg
	}
	return f
}

// MaybeHideFuncTS - use closure as a data storage - thread safe
func MaybeHideFuncTS(shas map[string]string) (f func(string) string) {
	cache := make(map[string]string)
	mtx := &sync.RWMutex{}
	smtx := &sync.Mutex{}
	f = func(arg string) string {
		var sha string
		mtx.RLock()
		sha, ok := cache[arg]
		mtx.RUnlock()
		if !ok {
			hash := sha1.New()
			_, err := hash.Write([]byte(arg))
			FatalOnError(err)
			sha = hex.EncodeToString(hash.Sum(nil))
			mtx.Lock()
			cache[arg] = sha
			mtx.Unlock()
		}
		smtx.Lock()
		anon, ok := shas[sha]
		smtx.Unlock()
		if ok {
			return anon
		}
		return arg
	}
	return f
}

// RandString - return random string
func RandString() string {
	return fmt.Sprintf("%x", rand.Uint64())
}

// FormatRawBytes - format []uint8 string
func FormatRawBytes(rawB []uint8) string {
	raw := fmt.Sprintf("%v", reflect.ValueOf(rawB))
	op := strings.Index(raw, "[") + 1
	cl := strings.Index(raw, "]")
	ary := strings.Split(raw[op:cl], " ")
	formatted := ""
	for _, s := range ary {
		b, _ := strconv.ParseInt(s, 10, 32)
		formatted += fmt.Sprintf("%02x", b)
	}
	return fmt.Sprintf("%T(%d)", rawB, len(rawB)) + ":" + formatted + ":" + fmt.Sprintf("%+v", rawB)
}

// FormatRawInterface - format raw string that is probably value or pointer to either []uint8 or sql.RawBytes
func FormatRawInterface(rawI interface{}) string {
	raw := fmt.Sprintf("%v", reflect.ValueOf(rawI))
	op := strings.Index(raw, "[") + 1
	cl := strings.Index(raw, "]")
	ary := strings.Split(raw[op:cl], " ")
	formatted := ""
	for _, s := range ary {
		b, _ := strconv.ParseInt(s, 10, 32)
		formatted += fmt.Sprintf("%02x", b)
	}
	return fmt.Sprintf("%T", rawI) + ":" + formatted + ":" + fmt.Sprintf("%+v", rawI)
}

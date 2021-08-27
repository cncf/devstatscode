package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// You should use files like 'graduated.secret' - they start from first graduated '    <tr>' line
// and end on last line before first inclubating line '    <tr>'
func tsplit(size int, kind, in string) (out string) {
	ary := strings.Split(in, "\n")
	lines := []string{}
	for _, item := range ary {
		if strings.TrimSpace(item) == "" {
			continue
		}
		lines = append(lines, item)
	}
	offset := ""
	for _, line := range lines {
		if strings.TrimSpace(line) == "<tr>" {
			off := strings.Index(line, "<tr>")
			if off > 0 {
				offset = line[:off]
			}
			break
		}
	}
	skips := []string{"<tr>", "</tr>", "colspan"}
	imageLines, linkLines := []string{}, []string{}
	for _, line := range lines {
		skipLine := false
		for _, skip := range skips {
			if strings.Contains(line, skip) {
				skipLine = true
				break
			}
		}
		if skipLine {
			continue
		}
		// fmt.Printf("considering line: %s\n", line)
		imgLine := strings.Contains(line, `class="cncf-proj"`)
		if imgLine {
			imageLines = append(imageLines, line)
		} else {
			linkLines = append(linkLines, line)
		}
	}
	linkReplacer := strings.NewReplacer(` class="cncf-bl"`, ``, ` class="cncf-br"`, ``, ` class="cncf-bl cncf-br"`, ``, ` class="cncf-br cncf-bl"`, ``)
	for i, line := range linkLines {
		linkLines[i] = linkReplacer.Replace(line)
	}
	imageReplacer := strings.NewReplacer(`class="cncf-bb cncf-bl"`, `class="cncf-bb"`, `class="cncf-bb cncf-br"`, `class="cncf-bb"`, `class="cncf-bb cncf-bl cncf-br"`, `class="cncf-bb"`, `class="cncf-bb cncf-br cncf-bl"`, `class="cncf-bb"`)
	for i, line := range imageLines {
		imageLines[i] = imageReplacer.Replace(line)
	}
	nItems := len(imageLines)
	nSections := nItems / size
	if nItems%size != 0 {
		nSections++
	}
	outLines := []string{}
	for section := 0; section < nSections; section++ {
		from := section * size
		to := from + size
		if to > nItems {
			to = nItems
		}
		n := to - from
		// fmt.Printf("section %d: %d-%d (%d items)\n", section, from, to, n)
		outLines = append(outLines, offset+"<tr>")
		outLines = append(outLines, offset+fmt.Sprintf(`  <td colspan="%d" class="cncf-sep">%s</td>`, n, kind))
		outLines = append(outLines, offset+"</tr>")
		outLines = append(outLines, offset+"<tr>")
		lastTo := to - 1
		for i := from; i < to; i++ {
			if i == from && i == lastTo {
				outLines = append(outLines, strings.Replace(linkLines[i], "<td>", `<td class="cncf-bl cncf-br">`, -1))
				continue
			}
			if i == from {
				outLines = append(outLines, strings.Replace(linkLines[i], "<td>", `<td class="cncf-bl">`, -1))
				continue
			}
			if i == lastTo {
				outLines = append(outLines, strings.Replace(linkLines[i], "<td>", `<td class="cncf-br">`, -1))
				continue
			}
			outLines = append(outLines, linkLines[i])
		}
		outLines = append(outLines, offset+"</tr>")
		outLines = append(outLines, offset+"<tr>")
		for i := from; i < to; i++ {
			if i == from && i == lastTo {
				outLines = append(outLines, strings.Replace(imageLines[i], `<td class="cncf-bb">`, `<td class="cncf-bb cncf-bl cncf-br">`, -1))
				continue
			}
			if i == from {
				outLines = append(outLines, strings.Replace(imageLines[i], `<td class="cncf-bb">`, `<td class="cncf-bb cncf-bl">`, -1))
				continue
			}
			if i == lastTo {
				outLines = append(outLines, strings.Replace(imageLines[i], `<td class="cncf-bb">`, `<td class="cncf-bb cncf-br">`, -1))
				continue
			}
			outLines = append(outLines, imageLines[i])
		}
		outLines = append(outLines, offset+"</tr>")
	}
	// fmt.Printf("Links:\n%s\n", strings.Join(linkLines, "\n"))
	// fmt.Printf("Images:\n%s\n", strings.Join(imageLines, "\n"))
	out = strings.Join(outLines, "\n")
	return
}

func main() {
	kind := os.Getenv("KIND")
	if kind == "" {
		fmt.Printf("You need to specify kind via KIND=Graduated|Incubating|Sandbox\n")
		return
	}
	ssize := os.Getenv("SIZE")
	if ssize == "" {
		fmt.Printf("You need to specify size via SIZE=n (usually 9, 10, 11, 12)\n")
		return
	}
	size, err := strconv.Atoi(ssize)
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		return
	}
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		return
	}
	fmt.Printf("%s\n", tsplit(size, kind, string(data)))
}

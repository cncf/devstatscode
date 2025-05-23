package devstatscode

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Annotations contain list of annotations
type Annotations struct {
	Annotations []Annotation
}

// Annotation contain each annotation data
type Annotation struct {
	Name        string
	Description string
	Date        time.Time
}

// AnnotationsByDate annotations Sort interface
type AnnotationsByDate []Annotation

func (a AnnotationsByDate) Len() int {
	return len(a)
}
func (a AnnotationsByDate) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a AnnotationsByDate) Less(i, j int) bool {
	return a[i].Date.Before(a[j].Date)
}

// GetFakeAnnotations - returns 'startDate - joinDate' and 'joinDate - now' annotations
func GetFakeAnnotations(startDate, joinDate time.Time) (annotations Annotations) {
	minDate := TimeParseAny("2012-07-01")
	if joinDate.Before(minDate) || startDate.Before(minDate) || !joinDate.After(startDate) {
		return
	}
	annotations.Annotations = append(
		annotations.Annotations,
		Annotation{
			Name:        "Project start",
			Description: ToYMDDate(startDate) + " - project starts",
			Date:        startDate,
		},
	)
	annotations.Annotations = append(
		annotations.Annotations,
		Annotation{
			Name:        "First CNCF project join date",
			Description: ToYMDDate(joinDate),
			Date:        joinDate,
		},
	)
	return
}

// GetAnnotations queries uses `git` to get `orgRepo` all tags list
// for all tags and returns those matching `annoRegexp`
func GetAnnotations(ctx *Ctx, orgRepo, annoRegexp string) (annotations Annotations) {
	// Get org and repo from orgRepo
	ary := strings.Split(orgRepo, "/")
	if len(ary) != 2 {
		Fatalf("main repository format must be 'org/repo', found '%s'", orgRepo)
	}

	// Compile annotation regexp if present, if no regexp then return all tags
	var re *regexp.Regexp
	if annoRegexp != "" {
		re = regexp.MustCompile(annoRegexp)
	}

	// Local or cron mode?
	cmdPrefix := ""
	if ctx.LocalCmd {
		cmdPrefix = LocalGitScripts
	}

	// We need this to capture 'git_tags.sh' output.
	ctx.ExecOutput = true

	// Get tags is using shell script that does 'chdir'
	// We cannot chdir because this is a multithreaded app
	// And all threads share CWD (current working directory)
	if ctx.Debug > 0 {
		Printf("Getting tags for repo %s\n", orgRepo)
	}
	dtStart := time.Now()
	rwd := ctx.ReposDir + orgRepo
	tagsStr, err := ExecCommand(
		ctx,
		[]string{cmdPrefix + "git_tags.sh", rwd},
		map[string]string{"GIT_TERMINAL_PROMPT": "0"},
	)
	dtEnd := time.Now()
	FatalOnError(err)

	tags := strings.Split(tagsStr, "\n")
	nTags := 0

	minDate := TimeParseAny("2012-07-01")
	var anns Annotations
	for _, tagData := range tags {
		data := strings.TrimSpace(tagData)
		if data == "" {
			continue
		}
		// Use '♂♀' separator to avoid any character that can appear inside tag name or description
		tagDataAry := strings.Split(data, "♂♀")
		if len(tagDataAry) != 3 {
			Fatalf("invalid tagData returned for repo: %s: '%s'", orgRepo, data)
		}
		tagName := tagDataAry[0]
		if re != nil && !re.MatchString(tagName) {
			continue
		}
		if tagDataAry[1] == "" {
			if ctx.Debug > 0 {
				Printf("Empty time returned for repo: %s, tag: %s\n", orgRepo, tagName)
			}
			continue
		}
		unixTimeStamp, err := strconv.ParseInt(tagDataAry[1], 10, 64)
		if err != nil {
			Printf("Invalid time returned for repo: %s, tag: %s: '%s'\n", orgRepo, tagName, data)
			continue
		}
		FatalOnError(err)
		creatorDate := time.Unix(unixTimeStamp, 0)
		if creatorDate.Before(minDate) {
			if ctx.Debug > 0 {
				Printf("Skipping annotation %v because it is before %v\n", creatorDate, minDate)
			}
			continue
		}
		message := tagDataAry[2]
		if len(message) > 40 {
			message = message[0:40]
		}
		replacer := strings.NewReplacer("\n", " ", "\r", " ", "\t", " ")
		message = replacer.Replace(message)

		anns.Annotations = append(
			anns.Annotations,
			Annotation{
				Name:        tagName,
				Description: message,
				Date:        creatorDate,
			},
		)
		nTags++
	}

	if ctx.Debug > 0 {
		Printf("Got %d tags for %s, took %v\n", nTags, orgRepo, dtEnd.Sub(dtStart))
	}

	// Remove duplicates (annotations falling into the same hour)
	prevHourDate := minDate
	sort.Sort(AnnotationsByDate(anns.Annotations))
	for _, ann := range anns.Annotations {
		currHourDate := HourStart(ann.Date)
		if currHourDate == prevHourDate {
			if ctx.Debug > 0 {
				Printf("Skipping annotation %v because its hour date is the same as the previous one\n", ann)
			}
			continue
		}
		prevHourDate = currHourDate
		annotations.Annotations = append(annotations.Annotations, ann)
	}

	return
}

// ProcessAnnotations Creates annotations and quick_series
func ProcessAnnotations(ctx *Ctx, annotations *Annotations, dates []*time.Time) {
	// Connect to Postgres
	ic := PgConn(ctx)
	defer func() { FatalOnError(ic.Close()) }()

	// CNCF milestone dates
	startDate := dates[0]
	joinDate := dates[1]
	incubatingDate := dates[2]
	graduatedDate := dates[3]
	archivedDate := dates[4]

	// Get BatchPoints
	var pts TSPoints

	// Annotations must be sorted to create quick ranges
	sort.Sort(AnnotationsByDate(annotations.Annotations))

	// Iterate annotations
	for _, annotation := range annotations.Annotations {
		annotationName := SafeUTF8String(annotation.Name)
		annotationDescription := SafeUTF8String(annotation.Description)

		fields := map[string]interface{}{
			"title":       annotationName,
			"description": annotationDescription,
		}
		// Add batch point
		if ctx.Debug > 0 {
			Printf(
				"Series: %v: Date: %v: '%v', '%v'\n",
				"annotations",
				ToYMDDate(annotation.Date),
				annotation.Name,
				annotation.Description,
			)
		}
		pt := NewTSPoint(ctx, "annotations", "", nil, fields, annotation.Date, false)
		AddTSPoint(ctx, &pts, pt)
	}

	// If both start and join dates are present then join date must be after start date
	if startDate == nil || joinDate == nil || (startDate != nil && joinDate != nil && joinDate.After(*startDate)) {
		// Project start date (additional annotation not used in quick ranges)
		if startDate != nil {
			fields := map[string]interface{}{
				"title":       "Project start date",
				"description": ToYMDDate(*startDate) + " - project starts",
			}
			// Add batch point
			if ctx.Debug > 0 {
				Printf(
					"Project start date: %v: '%v', '%v'\n",
					ToYMDDate(*startDate),
					fields["title"],
					fields["description"],
				)
			}
			pt := NewTSPoint(ctx, "annotations", "", nil, fields, *startDate, false)
			AddTSPoint(ctx, &pts, pt)
		}

		// Join CNCF (additional annotation not used in quick ranges)
		if joinDate != nil {
			fields := map[string]interface{}{
				"title":       "CNCF join date",
				"description": ToYMDDate(*joinDate) + " - joined CNCF",
			}
			// Add batch point
			if ctx.Debug > 0 {
				Printf(
					"CNCF join date: %v: '%v', '%v'\n",
					ToYMDDate(*joinDate),
					fields["title"],
					fields["description"],
				)
			}
			pt := NewTSPoint(ctx, "annotations", "", nil, fields, *joinDate, false)
			AddTSPoint(ctx, &pts, pt)
		}
	}

	// Moved to Incubating
	if incubatingDate != nil {
		fields := map[string]interface{}{
			"title":       "Moved to incubating state",
			"description": ToYMDDate(*incubatingDate) + " - project moved to incubating state",
		}
		// Add batch point
		if ctx.Debug > 0 {
			Printf(
				"Project moved to incubating state: %v: '%v', '%v'\n",
				ToYMDDate(*incubatingDate),
				fields["title"],
				fields["description"],
			)
		}
		pt := NewTSPoint(ctx, "annotations", "", nil, fields, *incubatingDate, false)
		AddTSPoint(ctx, &pts, pt)
	}

	// Graduated
	if graduatedDate != nil {
		fields := map[string]interface{}{
			"title":       "Graduated",
			"description": ToYMDDate(*graduatedDate) + " - project graduated",
		}
		// Add batch point
		if ctx.Debug > 0 {
			Printf(
				"Project graduated: %v: '%v', '%v'\n",
				ToYMDDate(*graduatedDate),
				fields["title"],
				fields["description"],
			)
		}
		pt := NewTSPoint(ctx, "annotations", "", nil, fields, *graduatedDate, false)
		AddTSPoint(ctx, &pts, pt)
	}

	// Archived
	if archivedDate != nil {
		fields := map[string]interface{}{
			"title":       "Archived",
			"description": ToYMDDate(*archivedDate) + " - project was archived",
		}
		// Add batch point
		if ctx.Debug > 0 {
			Printf(
				"Project was archived: %v: '%v', '%v'\n",
				ToYMDDate(*archivedDate),
				fields["title"],
				fields["description"],
			)
		}
		pt := NewTSPoint(ctx, "annotations", "", nil, fields, *archivedDate, false)
		AddTSPoint(ctx, &pts, pt)
	}

	// Special ranges
	periods := [][3]string{
		{"d", "Last day", "1 day"},
		{"w", "Last week", "1 week"},
		{"d10", "Last 10 days", "10 days"},
		{"m", "Last month", "1 month"},
		{"q", "Last quarter", "3 months"},
		{"m6", "Last 6 months", "6 months"},
		{"y", "Last year", "1 year"},
		{"y2", "Last 2 years", "2 years"},
		{"y3", "Last 3 years", "3 years"},
		{"y5", "Last 5 years", "5 years"},
		{"y10", "Last decade", "10 years"},
		{"y100", "Last century", "100 years"},
	}

	// tags:
	// suffix: will be used as TS series name suffix and Grafana drop-down value (non-dsplayed)
	// name: will be used as Grafana drop-down value name
	// data: is suffix;period;from;to
	// period: only for special values listed here, last ... week, day, quarter, devade etc - will be passed to Postgres
	// from: only filled when using annotations range - exact date from
	// to: only filled when using annotations range - exact date to
	tags := make(map[string]string)

	// Add special periods
	tagName := "quick_ranges"
	tm := TimeParseAny("2012-07-01")

	// Last "..." periods
	for _, period := range periods {
		tags[tagName+"_suffix"] = period[0]
		tags[tagName+"_name"] = period[1]
		tags[tagName+"_data"] = period[0] + ";" + period[2] + ";;"
		if ctx.Debug > 0 {
			Printf(
				"Series: %v: %+v\n",
				tagName,
				tags,
			)
		}
		// Add batch point
		pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
		AddTSPoint(ctx, &pts, pt)
		tm = tm.Add(time.Hour)
	}

	// Add '(i) - (i+1)' annotation ranges
	lastIndex := len(annotations.Annotations) - 1
	for index, annotation := range annotations.Annotations {
		if index == lastIndex {
			sfx := fmt.Sprintf("a_%d_n", index)
			annotationName := SafeUTF8String(annotation.Name)
			tags[tagName+"_suffix"] = sfx
			tags[tagName+"_name"] = fmt.Sprintf("%s - now", annotationName)
			tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(annotation.Date), ToYMDHMSDate(NextDayStart(time.Now())))
			if ctx.Debug > 0 {
				Printf(
					"Series: %v: %+v\n",
					tagName,
					tags,
				)
			}
			// Add batch point
			pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
			AddTSPoint(ctx, &pts, pt)
			tm = tm.Add(time.Hour)
			break
		}
		nextAnnotation := annotations.Annotations[index+1]
		sfx := fmt.Sprintf("a_%d_%d", index, index+1)
		annotationName := SafeUTF8String(annotation.Name)
		nextAnnotationName := SafeUTF8String(nextAnnotation.Name)
		tags[tagName+"_suffix"] = sfx
		tags[tagName+"_name"] = fmt.Sprintf("%s - %s", annotationName, nextAnnotationName)
		tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(annotation.Date), ToYMDHMSDate(nextAnnotation.Date))
		if ctx.Debug > 0 {
			Printf(
				"Series: %v: %+v\n",
				tagName,
				tags,
			)
		}
		// Add batch point
		pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
		AddTSPoint(ctx, &pts, pt)
		tm = tm.Add(time.Hour)
	}

	// 2 special periods: before and after joining CNCF
	if startDate != nil && joinDate != nil && joinDate.After(*startDate) {
		// From project start to CNCF join date
		sfx := "c_b"
		tags[tagName+"_suffix"] = sfx
		tags[tagName+"_name"] = "Before joining CNCF"
		tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*startDate), ToYMDHMSDate(*joinDate))
		if ctx.Debug > 0 {
			Printf(
				"Series: %v: %+v\n",
				tagName,
				tags,
			)
		}
		// Add batch point
		pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
		AddTSPoint(ctx, &pts, pt)
		tm = tm.Add(time.Hour)

		// From CNCF join date till now
		sfx = "c_n"
		tags[tagName+"_suffix"] = sfx
		tags[tagName+"_name"] = "Since joining CNCF"
		tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*joinDate), ToYMDHMSDate(NextDayStart(time.Now())))
		if ctx.Debug > 0 {
			Printf(
				"Series: %v: %+v\n",
				tagName,
				tags,
			)
		}
		// Add batch point
		pt = NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
		AddTSPoint(ctx, &pts, pt)
		tm = tm.Add(time.Hour)

		// If we have both moved to incubating and graduation, then graduation must happen after moving to incubation
		correctOrder := true
		if incubatingDate != nil && graduatedDate != nil && !graduatedDate.After(*incubatingDate) {
			correctOrder = false
		}

		// Moved to incubating handle
		if correctOrder == true && incubatingDate != nil && incubatingDate.After(*joinDate) {
			// From CNCF join date to incubating date
			sfx := "c_j_i"
			tags[tagName+"_suffix"] = sfx
			tags[tagName+"_name"] = "CNCF join date - moved to incubation"
			tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*joinDate), ToYMDHMSDate(*incubatingDate))
			if ctx.Debug > 0 {
				Printf(
					"Series: %v: %+v\n",
					tagName,
					tags,
				)
			}
			// Add batch point
			pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
			AddTSPoint(ctx, &pts, pt)
			tm = tm.Add(time.Hour)

			// From incubating till graduating or now
			if graduatedDate != nil {
				// From incubating date to graduated date
				sfx := "c_i_g"
				tags[tagName+"_suffix"] = sfx
				tags[tagName+"_name"] = "Moved to incubation - graduated"
				tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*incubatingDate), ToYMDHMSDate(*graduatedDate))
				if ctx.Debug > 0 {
					Printf(
						"Series: %v: %+v\n",
						tagName,
						tags,
					)
				}
				// Add batch point
				pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
				AddTSPoint(ctx, &pts, pt)
			} else {
				// From incubating till now
				sfx = "c_i_n"
				tags[tagName+"_suffix"] = sfx
				tags[tagName+"_name"] = "Since moving to incubating state"
				tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*incubatingDate), ToYMDHMSDate(NextDayStart(time.Now())))
				if ctx.Debug > 0 {
					Printf(
						"Series: %v: %+v\n",
						tagName,
						tags,
					)
				}
				// Add batch point
				pt = NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
				AddTSPoint(ctx, &pts, pt)
			}
			tm = tm.Add(time.Hour)
		}

		// Graduated handle
		if correctOrder == true && graduatedDate != nil && graduatedDate.After(*joinDate) {
			// If incubating happened after graduation or there was no moved to incubating date
			if incubatingDate == nil {
				// From CNCF join date to graduated
				sfx := "c_j_g"
				tags[tagName+"_suffix"] = sfx
				tags[tagName+"_name"] = "CNCF join date - graduated"
				tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*joinDate), ToYMDHMSDate(*graduatedDate))
				if ctx.Debug > 0 {
					Printf(
						"Series: %v: %+v\n",
						tagName,
						tags,
					)
				}
				// Add batch point
				pt := NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
				AddTSPoint(ctx, &pts, pt)
				tm = tm.Add(time.Hour)
			}
			// From graduated till now
			sfx = "c_g_n"
			tags[tagName+"_suffix"] = sfx
			tags[tagName+"_name"] = "Since graduating"
			tags[tagName+"_data"] = fmt.Sprintf("%s;;%s;%s", sfx, ToYMDHMSDate(*graduatedDate), ToYMDHMSDate(NextDayStart(time.Now())))
			if ctx.Debug > 0 {
				Printf(
					"Series: %v: %+v\n",
					tagName,
					tags,
				)
			}
			// Add batch point
			pt = NewTSPoint(ctx, tagName, "", tags, nil, tm, false)
			AddTSPoint(ctx, &pts, pt)
			tm = tm.Add(time.Hour)
		}
	}

	// Write the batch
	if !ctx.SkipTSDB {
		table := "tquick_ranges"
		column := "quick_ranges_suffix"
		if TableExists(ic, ctx, table) && TableColumnExists(ic, ctx, table, column) {
			ExecSQLWithErr(ic, ctx, fmt.Sprintf("delete from \"%s\" where \"%s\" like '%%_n'", table, column))
		}
		WriteTSPoints(ctx, ic, &pts, "", []uint8{}, nil)
		// Annotations from all projects into 'allprj' database
		if !ctx.SkipSharedDB && ctx.SharedDB != "" {
			var anots TSPoints
			for _, pt := range pts {
				if pt.name != "annotations" {
					continue
				}
				pt.name = "annotations_shared"
				if pt.fields != nil {
					pt.period = ctx.Project
					pt.fields["repo"] = ctx.ProjectMainRepo
				}
				anots = append(anots, pt)
			}
			ics := PgConnDB(ctx, ctx.SharedDB)
			defer func() { FatalOnError(ics.Close()) }()
			WriteTSPoints(ctx, ics, &anots, "", []uint8{}, nil)
		}
	} else if ctx.Debug > 0 {
		Printf("Skipping annotations series write\n")
	}
}

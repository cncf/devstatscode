package devstatscode

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic"
)

// ES - ElasticSearch connection client, context and default mapping
type ES struct {
	ctx           context.Context
	es            *elastic.Client
	mapping       string
	mappingRaw    string
	prefix        string
	fieldsToMerge map[string]string
}

// ESDataObject internal JSON data for stored documents
type ESDataObject struct {
	Name    string    `json:"name"`
	IValue  float64   `json:"ivalue"`
	SValue  string    `json:"svalue"`
	SValue2 string    `json:"svalue2"`
	SValue3 string    `json:"svalue3"`
	DtValue time.Time `json:"dtvalue"`
}

// ESBulks keeps array of bulk services to add/delete
// each delete/add but can hold 10 items
type ESBulks struct {
	add     []*elastic.BulkService
	del     []*elastic.BulkService
	currAdd *elastic.BulkService
	currDel *elastic.BulkService
	nBulks  int
	nItems  int
	k       int
	max     int
}

// Init creates structure to hanle bulk inserts/deletes
func (b *ESBulks) Init(ec *elastic.Client, ctx *Ctx) {
	b.add = append(b.add, ec.Bulk())
	b.del = append(b.del, ec.Bulk())
	b.currAdd = b.add[0]
	b.currDel = b.del[0]
	b.nBulks = 1
	b.max = ctx.ESBulkSize
}

// CurrentDel returns current bulk delete object
func (b *ESBulks) CurrentDel() *elastic.BulkService {
	return b.currDel
}

// CurrentAdd returns current bulk add object
func (b *ESBulks) CurrentAdd() *elastic.BulkService {
	return b.currAdd
}

// Next will increase objects count and possibly switch to another bulk objects
func (b *ESBulks) Next(ec *elastic.Client) {
	b.nItems++
	b.k++
	if b.k == b.max {
		b.k = 0
		b.add = append(b.add, ec.Bulk())
		b.del = append(b.del, ec.Bulk())
		b.currAdd = b.add[b.nBulks]
		b.currDel = b.del[b.nBulks]
		b.nBulks++
	}
}

// String - output bulks config
func (b *ESBulks) String() string {
	return fmt.Sprintf("{nBulks:%d, nItems:%d, k:%d, max:%d}", b.nBulks, b.nItems, b.k, b.max)
}

// ESConn Connects to ElasticSearch
func ESConn(ctx *Ctx, prefix string) *ES {
	ctxb := context.Background()
	if ctx.QOut {
		Printf("ESConnectString: %s\n", ctx.ElasticURL)
	}
	// TODO: set sniff enable/disable via context var?
	client, err := elastic.NewClient(
		elastic.SetURL(ctx.ElasticURL),
		elastic.SetSniff(false),
		//elastic.SetScheme("https"),
	)
	FatalOnError(err)
	info, code, err := client.Ping(ctx.ElasticURL).Do(ctxb)
	FatalOnError(err)
	if ctx.Debug > 0 {
		Printf("ElasticSearch connection code %d and version %s\n", code, info.Version.Number)
	}
	fieldsToMerge := map[string]string{
		"name":  "svalue",
		"value": "ivalue",
		"descr": "svalue2",
		"dt":    "dtvalue",
		"str":   "svalue3",
	}
	return &ES{
		ctx:           ctxb,
		es:            client,
		prefix:        prefix,
		fieldsToMerge: fieldsToMerge,
		mapping: `{"settings":{"number_of_shards":5,"number_of_replicas":0},` +
			`"mappings":{"_doc":{` +
			`"dynamic_templates":[` +
			`{"not_analyzerd":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},` +
			`{"numbers":{"match":"*","match_mapping_type":"long","mapping":{"type":"float"}}}` +
			`],"properties":{` +
			`"type":{"type":"keyword"},` +
			`"time":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},` +
			`"series":{"type":"keyword"},` +
			`"period":{"type":"keyword"},` +
			`"descr":{"type":"keyword"},` +
			`"str":{"type":"keyword"},` +
			`"name":{"type":"keyword"},` +
			`"svalue":{"type":"keyword"},` +
			`"svalue2":{"type":"keyword"},` +
			`"svalue3":{"type":"keyword"},` +
			`"ivalue":{"type":"double"},` +
			`"dtvalue":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},` +
			`"data.svalue":{"type":"keyword"},` +
			`"data.svalue2":{"type":"keyword"},` +
			`"data.svalue3":{"type":"keyword"},` +
			`"data.ivalue":{"type":"double"},` +
			`"data.dtvalue":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},` +
			`"value":{"type":"double"}` +
			`}}}}`,
		mappingRaw: `{"settings":{"number_of_shards":5,"number_of_replicas":0},` +
			`"mappings":{"_doc":{` +
			`"dynamic_templates":[` +
			`{"not_analyzerd":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},` +
			`{"numbers":{"match":"*","match_mapping_type":"long","mapping":{"type":"float"}}}` +
			`],"properties":{` +
			`"type":{"type":"keyword"},` +
			// `"message":{"type":"text"},` +
			// `"title":{"type":"text"},` +
			// `"body":{"type":"text"},` +
			`"full_body":{"type":"text"},` +
			`"time":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"}` +
			`}}}}`,
	}
}

// ESIndexName returns ES index name "d_{{project}}" --> "d_kubernetes"
func (es *ES) ESIndexName(ctx *Ctx) string {
	if ctx.Project == "" {
		Fatalf("you need to specify project via GHA2DB_PROJECT=...")
	}
	return es.prefix + ctx.Project
}

// ESEscapeFieldName escape characters non allowed in ES field names
func (es *ES) ESEscapeFieldName(fieldName string) string {
	return strings.Replace(fieldName, ".", "", -1)
}

// IndexExists checks if index exists
func (es *ES) IndexExists(ctx *Ctx) bool {
	exists, err := es.es.IndexExists(es.ESIndexName(ctx)).Do(es.ctx)
	FatalOnError(err)
	return exists
}

// CreateIndex creates index
func (es *ES) CreateIndex(ctx *Ctx, raw bool) {
	var mapping string
	if raw {
		mapping = es.mappingRaw
	} else {
		mapping = es.mapping
	}
	createIndex, err := es.es.CreateIndex(es.ESIndexName(ctx)).BodyString(mapping).Do(es.ctx)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		if ctx.Debug > 0 {
			Printf("CreateIndex: %s index already exists: %+v\n", es.ESIndexName(ctx), err)
		}
		return
	}
	FatalOnError(err)
	if !createIndex.Acknowledged {
		Fatalf("index " + es.ESIndexName(ctx) + " not created")
	}
}

// DeleteByQuery deletes data from given index & type by simple bool query
func (es *ES) DeleteByQuery(ctx *Ctx, propNames []string, propValues []interface{}) {
	boolQuery := elastic.NewBoolQuery()
	for i := range propNames {
		boolQuery = boolQuery.Must(elastic.NewTermQuery(propNames[i], propValues[i]))
	}
	ne := 0
	for {
		result, err := elastic.NewDeleteByQueryService(es.es).Index(es.ESIndexName(ctx)).Type("_doc").Query(boolQuery).Do(es.ctx)
		if err != nil && strings.Contains(err.Error(), "search_phase_execution_exception") {
			if ctx.Debug > 0 {
				Printf("DeleteByQuery: %s index not yet ready for delete (so it doesn't have data for delete anyway): %+v\n", es.ESIndexName(ctx), err)
			}
			return
		}
		if err != nil && strings.Contains(err.Error(), "Error 409 (Conflict)") && ne < 100 {
			time.Sleep(time.Duration(20000000) * time.Nanosecond)
			ne++
			continue
		}
		FatalOnError(err)
		if ctx.Debug > 0 {
			Printf("DeleteByQuery(%+v, %+v): %+v\n", propNames, propValues, result)
		}
		break
	}
}

// DeleteByWildcardQuery deletes data from given index & type by using wildcard query
func (es *ES) DeleteByWildcardQuery(ctx *Ctx, propName, propQuery string) {
	wildcardQuery := elastic.NewWildcardQuery(propName, propQuery)
	ne := 0
	for {
		result, err := elastic.NewDeleteByQueryService(es.es).Index(es.ESIndexName(ctx)).Type("_doc").Query(wildcardQuery).Do(es.ctx)
		if err != nil && strings.Contains(err.Error(), "search_phase_execution_exception") {
			if ctx.Debug > 0 {
				Printf("DeleteByWildcardQuery: %s index not yet ready for delete (so it doesn't have data for delete anyway): %+v\n", es.ESIndexName(ctx), err)
			}
			return
		}
		if err != nil && strings.Contains(err.Error(), "Error 409 (Conflict)") && ne < 100 {
			time.Sleep(time.Duration(20000000) * time.Nanosecond)
			ne++
			continue
		}
		FatalOnError(err)
		if ctx.Debug > 0 {
			Printf("DeleteByWildcardQuery(%s, %s): %+v\n", propName, propQuery, result)
		}
		break
	}
}

// GetElasticClient - returns embedded ES client
func (es *ES) GetElasticClient() *elastic.Client {
	return es.es
}

// Bulks returns Delete and Add requests
func (es *ES) Bulks() (*elastic.BulkService, *elastic.BulkService) {
	return es.es.Bulk(), es.es.Bulk()
}

// AddBulksItems adds items to the Bulk Request
func (es *ES) AddBulksItems(ctx *Ctx, b *ESBulks, doc map[string]interface{}, keys []string) {
	docHash := HashObject(doc, keys)
	b.CurrentDel().Add(elastic.NewBulkDeleteRequest().Index(es.ESIndexName(ctx)).Type("_doc").Id(docHash))
	b.CurrentAdd().Add(elastic.NewBulkIndexRequest().Index(es.ESIndexName(ctx)).Type("_doc").Doc(doc).Id(docHash))
	b.Next(es.es)
}

// AddBulksItemsI adds items to the Bulk Request
func (es *ES) AddBulksItemsI(ctx *Ctx, b *ESBulks, doc interface{}, docHash string) {
	b.CurrentDel().Add(elastic.NewBulkDeleteRequest().Index(es.ESIndexName(ctx)).Type("_doc").Id(docHash))
	b.CurrentAdd().Add(elastic.NewBulkIndexRequest().Index(es.ESIndexName(ctx)).Type("_doc").Doc(doc).Id(docHash))
	b.Next(es.es)
}

// ExecuteBulkDel executes scheduled commands (delete and then inserts)
func (es *ES) ExecuteBulkDel(ctx *Ctx, bulkDel *elastic.BulkService) {
	res, err := bulkDel.Do(es.ctx)
	if err != nil && strings.Contains(err.Error(), "No bulk actions to commit") {
		if ctx.Debug > 0 {
			Printf("ExecuteBulkDel: no actions to commit\n")
		}
	} else {
		actions := bulkDel.NumberOfActions()
		if actions != 0 {
			Printf("bulk delete: not all actions executed: %+v\n", actions)
			if err == nil {
				err = fmt.Errorf("bulk delete: not all actions executed: %+v", actions)
			}
		}
		failedResults := res.Failed()
		nFailed := len(failedResults)
		if len(failedResults) > 0 {
			for _, failed := range failedResults {
				if strings.Contains(failed.Result, "not_found") {
					nFailed--
				} else {
					Printf("Failed delete: %+v: %+v\n", failed, failed.Error)
				}
			}
			if nFailed > 0 {
				Printf("bulk delete failed: %+v\n", failedResults)
				if err == nil {
					err = fmt.Errorf("bulk delete failed: %+v", failedResults)
				}
			}
		}
		FatalOnError(err)
	}
}

// ExecuteBulkAdd executes scheduled commands (delete and then inserts)
func (es *ES) ExecuteBulkAdd(ctx *Ctx, bulkAdd *elastic.BulkService) {
	res, err := bulkAdd.Do(es.ctx)
	if err != nil && strings.Contains(err.Error(), "No bulk actions to commit") {
		if ctx.Debug > 0 {
			Printf("ExecuteBulkAdd: no actions to commit\n")
		}
	} else if err != nil && strings.Contains(err.Error(), "transport connection broken") {
		Printf("ERROR: ExecuteBulkAdd: transport connection broken, skipping: %+v\n", err)
		time.Sleep(2000 * time.Millisecond)
	} else if err != nil && strings.Contains(err.Error(), "context deadline exceeded") {
		Printf("ERROR: ExecuteBulkAdd: context deadline exceeded, skipping: %+v\n", err)
		time.Sleep(2000 * time.Millisecond)
	} else {
		actions := bulkAdd.NumberOfActions()
		if actions != 0 {
			Printf("bulk add not all actions executed: %+v\n", actions)
			if err == nil {
				err = fmt.Errorf("bulk add not all actions executed: %+v", actions)
			}
		}
		failedResults := res.Failed()
		if len(failedResults) > 0 {
			for _, failed := range failedResults {
				Printf("Failed add: %+v: %+v\n", failed, failed.Error)
			}
			Printf("bulk failed add: %+v\n", failedResults)
			if err == nil {
				err = fmt.Errorf("bulk failed add: %+v", failedResults)
			}
		}
		FatalOnError(err)
	}
}

// ExecuteBulks executes scheduled commands (delete and then inserts)
func (es *ES) ExecuteBulks(ctx *Ctx, b *ESBulks) {
	if ctx.Debug > 0 {
		Printf("%+v\n", b)
	}
	for _, del := range b.del {
		es.ExecuteBulkDel(ctx, del)
	}
	for _, add := range b.add {
		es.ExecuteBulkAdd(ctx, add)
	}
	b.add = []*elastic.BulkService{}
	b.del = []*elastic.BulkService{}
	b.currAdd = nil
	b.currDel = nil
	b.nBulks = 0
	b.nItems = 0
	b.k = 0
}

// WriteESPoints write batch of points to postgresql
// outputs[0] - output using variable column name (1 doc) [used by annotations, tags and vars]
// outputs[1] - output using data[] array containing {name,ivalue,svalue,svalue2,svalue3,dtvalue} (1 doc), any of those keys is optional [not used currently]
// outputs[2] - output using N separate docs, each containing {name,ivalue,svalue,svalue2,svalue3,dtvalue} (N docs) (but trying to keep both int and string value in the same record) [used by metrics/time-series]
func (es *ES) WriteESPoints(ctx *Ctx, pts *TSPoints, mergeS string, outputs [3]bool) {
	npts := len(*pts)
	if ctx.Debug > 0 {
		Printf("WriteESPoints: writing %d points\n", len(*pts))
		Printf("Points:\n%+v\n", pts.Str())
	}
	if npts == 0 {
		return
	}
	merge := false
	if mergeS != "" {
		mergeS = "s" + mergeS
		merge = true
	}
	// Create index
	exists := es.IndexExists(ctx)
	if !exists {
		es.CreateIndex(ctx, false)
	}
	items := 0

	// Handle Bulk operations
	var b ESBulks
	b.Init(es.es, ctx)

	for _, p := range *pts {
		if p.tags != nil {
			if outputs[0] || outputs[1] {
				obj := make(map[string]interface{})
				obj["type"] = "t" + p.name
				obj["time"] = ToYMDHMSDate(p.added)
				obj["tag_time"] = ToYMDHMSDate(p.t)
				data := []ESDataObject{}
				for tagName, tagValue := range p.tags {
					if outputs[0] {
						obj[es.ESEscapeFieldName(tagName)] = tagValue
					}
					if outputs[1] {
						data = append(data, ESDataObject{Name: tagName, SValue: tagValue})
					}
				}
				if outputs[1] {
					obj["data"] = data
				}
				es.AddBulksItems(ctx, &b, obj, []string{"type", "tag_time"})
				items++
			}
			if outputs[2] {
				for tagName, tagValue := range p.tags {
					obj := make(map[string]interface{})
					obj["type"] = "it" + p.name
					obj["time"] = ToYMDHMSDate(p.added)
					obj["tag_time"] = ToYMDHMSDate(p.t)
					obj["name"] = tagName
					obj["svalue"] = tagValue
					es.AddBulksItems(ctx, &b, obj, []string{"type", "tag_time", "name"})
					items++
				}
			}
		}
		if p.fields != nil && !merge {
			if outputs[0] || outputs[1] {
				obj := make(map[string]interface{})
				obj["type"] = "s" + p.name
				obj["time"] = ToYMDHMSDate(p.t)
				obj["period"] = p.period
				obj["time_added"] = ToYMDHMSDate(p.added)
				data := []ESDataObject{}
				for fieldName, fieldValue := range p.fields {
					if outputs[0] {
						obj[es.ESEscapeFieldName(fieldName)] = fieldValue
					}
					if outputs[1] {
						value, ok := fieldValue.(string)
						valueDt, okDt := fieldValue.(time.Time)
						if ok {
							data = append(data, ESDataObject{Name: fieldName, SValue: value})
						} else if okDt {
							data = append(data, ESDataObject{Name: fieldName, DtValue: valueDt})
						} else {
							value, ok := GetFloatFromInterface(fieldValue)
							if !ok {
								Fatalf("cannot convert %+v to a number", fieldValue)
							}
							data = append(data, ESDataObject{Name: fieldName, IValue: value})
						}
					}
				}
				if outputs[1] {
					obj["data"] = data
				}
				es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period"})
				items++
			}
			if outputs[2] {
				mergeFields := make(map[string]map[string]interface{})
				for fieldName, fieldValue := range p.fields {
					obj := make(map[string]interface{})
					obj["type"] = "is" + p.name
					obj["time"] = ToYMDHMSDate(p.t)
					obj["period"] = p.period
					obj["time_added"] = ToYMDHMSDate(p.added)
					obj["name"] = fieldName
					value, ok := fieldValue.(string)
					valueDt, okDt := fieldValue.(time.Time)
					if ok {
						field, ok := es.fieldsToMerge[fieldName]
						if ok {
							obj[field] = value
							mergeFields[field] = obj
							continue
						} else {
							obj["svalue"] = value
						}
					} else if okDt {
						obj["dtvalue"] = ToYMDHMSDate(valueDt)
					} else {
						value, ok := GetFloatFromInterface(fieldValue)
						if !ok {
							Fatalf("cannot convert %+v to a number", fieldValue)
						}
						obj["ivalue"] = value
					}
					field, ok := es.fieldsToMerge[fieldName]
					if ok {
						mergeFields[field] = obj
						continue
					}
					es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period", "name"})
					items++
				}
				if len(mergeFields) > 0 {
					var (
						initialized bool
					)
					obj := make(map[string]interface{})
					for merge, mobj := range mergeFields {
						if !initialized {
							for k, v := range mobj {
								obj[k] = v
							}
							initialized = true
						}
						obj[merge] = mobj[merge]
					}
					obj["name"] = Merged
					es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period", "name"})
					items++
				}
			}
		}
		if p.fields != nil && merge {
			if outputs[0] || outputs[1] {
				obj := make(map[string]interface{})
				obj["type"] = mergeS
				obj["time"] = ToYMDHMSDate(p.t)
				obj["period"] = p.period
				obj["series"] = p.name
				obj["time_added"] = ToYMDHMSDate(p.added)
				data := []ESDataObject{}
				for fieldName, fieldValue := range p.fields {
					if outputs[0] {
						obj[es.ESEscapeFieldName(fieldName)] = fieldValue
					}
					if outputs[1] {
						value, ok := fieldValue.(string)
						valueDt, okDt := fieldValue.(time.Time)
						if ok {
							data = append(data, ESDataObject{Name: fieldName, SValue: value})
						} else if okDt {
							data = append(data, ESDataObject{Name: fieldName, DtValue: valueDt})
						} else {
							value, ok := GetFloatFromInterface(fieldValue)
							if !ok {
								Fatalf("cannot convert %+v to a number", fieldValue)
							}
							data = append(data, ESDataObject{Name: fieldName, IValue: value})
						}
					}
				}
				if outputs[1] {
					obj["data"] = data
				}
				es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period", "series"})
				items++
			}
			if outputs[2] {
				mergeFields := make(map[string]map[string]interface{})
				for fieldName, fieldValue := range p.fields {
					obj := make(map[string]interface{})
					obj["type"] = "i" + mergeS
					obj["time"] = ToYMDHMSDate(p.t)
					obj["period"] = p.period
					obj["series"] = p.name
					obj["time_added"] = ToYMDHMSDate(p.added)
					obj["name"] = fieldName
					value, ok := fieldValue.(string)
					valueDt, okDt := fieldValue.(time.Time)
					if ok {
						field, ok := es.fieldsToMerge[fieldName]
						if ok {
							obj[field] = value
							mergeFields[field] = obj
							continue
						} else {
							obj["svalue"] = value
						}
					} else if okDt {
						obj["dtvalue"] = ToYMDHMSDate(valueDt)
					} else {
						value, ok := GetFloatFromInterface(fieldValue)
						if !ok {
							Fatalf("cannot convert %+v to a number", fieldValue)
						}
						obj["ivalue"] = value
					}
					field, ok := es.fieldsToMerge[fieldName]
					if ok {
						mergeFields[field] = obj
						continue
					}
					es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period", "series", "name"})
					items++
				}
				if len(mergeFields) > 0 {
					var (
						initialized bool
					)
					obj := make(map[string]interface{})
					for merge, mobj := range mergeFields {
						if !initialized {
							for k, v := range mobj {
								obj[k] = v
							}
							initialized = true
						}
						obj[merge] = mobj[merge]
					}
					obj["name"] = Merged
					es.AddBulksItems(ctx, &b, obj, []string{"type", "time", "period", "series", "name"})
					items++
				}
			}
		}
	}
	es.ExecuteBulks(ctx, &b)
	if ctx.Debug > 0 {
		Printf("Items: %d\n", items)
	}
}

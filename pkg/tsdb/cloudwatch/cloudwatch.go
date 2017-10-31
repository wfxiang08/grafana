package cloudwatch

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/tsdb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/grafana/grafana/pkg/components/null"
	"github.com/grafana/grafana/pkg/components/simplejson"
	"github.com/grafana/grafana/pkg/metrics"
)

type CloudWatchExecutor struct {
	*models.DataSource
}

type DatasourceInfo struct {
	Profile       string
	Region        string
	AuthType      string // 主要看是否为: "arn", 如果是 "arn"，则需要立马检查: AssumeRoleArn, 否则看 AccessKey/SecretKey
	AssumeRoleArn string
	Namespace     string

	AccessKey string
	SecretKey string
}

func NewCloudWatchExecutor(dsInfo *models.DataSource) (tsdb.TsdbQueryEndpoint, error) {
	// Data Source在执行Query是才设置
	return &CloudWatchExecutor{}, nil
}

var (
	plog               log.Logger
	standardStatistics map[string]bool
	aliasFormat        *regexp.Regexp
)

func init() {
	// 定制logger
	plog = log.New("tsdb.cloudwatch")

	// 注册Query Endpoint
	tsdb.RegisterTsdbQueryEndpoint("cloudwatch", NewCloudWatchExecutor)

	// 标准的统计指标
	standardStatistics = map[string]bool{
		"Average":     true,
		"Maximum":     true,
		"Minimum":     true,
		"Sum":         true,
		"SampleCount": true,
	}

	// alias的格式
	aliasFormat = regexp.MustCompile(`\{\{\s*(.+?)\s*\}\}`)
}

func (e *CloudWatchExecutor) Query(ctx context.Context, dsInfo *models.DataSource, queryContext *tsdb.TsdbQuery) (*tsdb.Response, error) {
	var result *tsdb.Response
	e.DataSource = dsInfo // 数据源

	queryType := queryContext.Queries[0].Model.Get("type").MustString("")
	var err error

	switch queryType {
	case "metricFindQuery":
		// Metric
		result, err = e.executeMetricFindQuery(ctx, queryContext)
		break
	case "annotationQuery":
		// Query
		result, err = e.executeAnnotationQuery(ctx, queryContext)
		break
	case "timeSeriesQuery":
		fallthrough
	default:
		result, err = e.executeTimeSeriesQuery(ctx, queryContext)
		break
	}

	return result, err
}

// 对于每一个指标，都异步去抓取数据
func (e *CloudWatchExecutor) executeTimeSeriesQuery(ctx context.Context, queryContext *tsdb.TsdbQuery) (*tsdb.Response, error) {
	result := &tsdb.Response{
		Results: make(map[string]*tsdb.QueryResult),
	}

	errCh := make(chan error, 1)
	resCh := make(chan *tsdb.QueryResult, 1)

	// 对于每一个指标，都异步去抓取数据
	currentlyExecuting := 0
	for i, model := range queryContext.Queries {
		queryType := model.Model.Get("type").MustString()
		if queryType != "timeSeriesQuery" && queryType != "" {
			continue
		}
		currentlyExecuting++
		go func(refId string, index int) {
			queryRes, err := e.executeQuery(ctx, queryContext.Queries[index].Model, queryContext)
			currentlyExecuting--
			if err != nil {
				errCh <- err
			} else {
				queryRes.RefId = refId
				resCh <- queryRes
			}
		}(model.RefId, i)
	}

	for currentlyExecuting != 0 {
		select {
		case res := <-resCh:
			result.Results[res.RefId] = res
		case err := <-errCh:
			return result, err
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}

	return result, nil
}

func (e *CloudWatchExecutor) executeQuery(ctx context.Context, parameters *simplejson.Json, queryContext *tsdb.TsdbQuery) (*tsdb.QueryResult, error) {
	query, err := parseQuery(parameters)
	if err != nil {
		return nil, err
	}

	// 实现类似的功能：
	//	aws cloudwatch get-metric-statistics --namespace AWS/RDS --dimensions  Name=DBInstanceIdentifier,Value=xxx-backend-readonly --metric-name ReadIOPS \
	//		--statistics Maximum --start-time 2017-10-30T00:18:00 --end-time 2017-10-30T12:48:00 --period 60

	// 获取client, 像我们自己的 AwsS3Client, 只不过这里是cloudwatch client
	client, err := e.getClient(query.Region)
	if err != nil {
		return nil, err
	}

	// --start-time
	startTime, err := queryContext.TimeRange.ParseFrom()
	if err != nil {
		return nil, err
	}

	// --end-time
	endTime, err := queryContext.TimeRange.ParseTo()
	if err != nil {
		return nil, err
	}

	params := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String(query.Namespace),    // 例如: AWS/RDS
		MetricName: aws.String(query.MetricName),   // 例如: ReadIOPS
		Dimensions: query.Dimensions,               // --dimensions 对应的参数
		Period:     aws.Int64(int64(query.Period)), // 60的倍数，单位s
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
	}
	if len(query.Statistics) > 0 {
		params.Statistics = query.Statistics
	}
	if len(query.ExtendedStatistics) > 0 {
		params.ExtendedStatistics = query.ExtendedStatistics
	}

	if setting.Env == setting.DEV {
		plog.Debug("CloudWatch query", "raw query", params)
	}

	// 调用API
	resp, err := client.GetMetricStatisticsWithContext(ctx, params, request.WithResponseReadTimeout(10*time.Second))
	if err != nil {
		return nil, err
	}
	// 计数器
	metrics.M_Aws_CloudWatch_GetMetricStatistics.Inc()

	queryRes, err := parseResponse(resp, query)
	if err != nil {
		return nil, err
	}

	return queryRes, nil
}

func parseDimensions(model *simplejson.Json) ([]*cloudwatch.Dimension, error) {
	var result []*cloudwatch.Dimension

	for k, v := range model.Get("dimensions").MustMap() {
		kk := k
		if vv, ok := v.(string); ok {
			result = append(result, &cloudwatch.Dimension{
				Name:  &kk,
				Value: &vv,
			})
		} else {
			return nil, errors.New("failed to parse")
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return *result[i].Name < *result[j].Name
	})
	return result, nil
}

func parseStatistics(model *simplejson.Json) ([]string, []string, error) {
	var statistics []string
	var extendedStatistics []string

	for _, s := range model.Get("statistics").MustArray() {
		if ss, ok := s.(string); ok {
			if _, isStandard := standardStatistics[ss]; isStandard {
				statistics = append(statistics, ss)
			} else {
				extendedStatistics = append(extendedStatistics, ss)
			}
		} else {
			return nil, nil, errors.New("failed to parse")
		}
	}

	return statistics, extendedStatistics, nil
}

func parseQuery(model *simplejson.Json) (*CloudWatchQuery, error) {
	region, err := model.Get("region").String()
	if err != nil {
		return nil, err
	}

	namespace, err := model.Get("namespace").String()
	if err != nil {
		return nil, err
	}

	metricName, err := model.Get("metricName").String()
	if err != nil {
		return nil, err
	}

	dimensions, err := parseDimensions(model)
	if err != nil {
		return nil, err
	}

	statistics, extendedStatistics, err := parseStatistics(model)
	if err != nil {
		return nil, err
	}

	p := model.Get("period").MustString("")
	if p == "" {
		// EC2最小时间单位: 5分钟
		if namespace == "AWS/EC2" {
			p = "300"
		} else {
			p = "60"
		}
	}

	// 如果p合法，则使用p; 否则默认5分钟
	period := 300
	if regexp.MustCompile(`^\d+$`).Match([]byte(p)) {
		period, err = strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
	} else {
		d, err := time.ParseDuration(p)
		if err != nil {
			return nil, err
		}
		period = int(d.Seconds())
	}

	// alias:
	alias := model.Get("alias").MustString("{{metric}}_{{stat}}") // 默认格式

	return &CloudWatchQuery{
		Region:             region,
		Namespace:          namespace,
		MetricName:         metricName,
		Dimensions:         dimensions,
		Statistics:         aws.StringSlice(statistics),
		ExtendedStatistics: aws.StringSlice(extendedStatistics),
		Period:             period,
		Alias:              alias,
	}, nil
}

func formatAlias(query *CloudWatchQuery, stat string, dimensions map[string]string) string {
	// 如何设置alias呢?
	data := map[string]string{}
	data["region"] = query.Region
	data["namespace"] = query.Namespace
	data["metric"] = query.MetricName
	data["stat"] = stat
	for k, v := range dimensions {
		data[k] = v
	}

	result := aliasFormat.ReplaceAllFunc([]byte(query.Alias), func(in []byte) []byte {
		labelName := strings.Replace(string(in), "{{", "", 1)
		labelName = strings.Replace(labelName, "}}", "", 1)
		labelName = strings.TrimSpace(labelName)
		if val, exists := data[labelName]; exists {
			return []byte(val)
		}

		return in
	})

	return string(result)
}

//
// 将cloudwatch的response解析成为tsdb的结果
//
func parseResponse(resp *cloudwatch.GetMetricStatisticsOutput, query *CloudWatchQuery) (*tsdb.QueryResult, error) {
	queryRes := tsdb.NewQueryResult()

	var value float64
	for _, s := range append(query.Statistics, query.ExtendedStatistics...) {
		// 每一个统计指标对应一个时间序列
		series := tsdb.TimeSeries{
			Tags: map[string]string{},
		}

		// 按照Dimensions来分Tags
		for _, d := range query.Dimensions {
			series.Tags[*d.Name] = *d.Value
		}

		// Alias这个很重要: 可以让图表简洁展示
		series.Name = formatAlias(query, *s, series.Tags)

		// TODO: 这个地方的代码还是挺SB的
		lastTimestamp := make(map[string]time.Time)

		// cloudwatch返回的数据是无序的，这里需要按照时间升序排列
		// 这个似乎可以提取到for loop完毕
		sort.Slice(resp.Datapoints, func(i, j int) bool {
			return (*resp.Datapoints[i].Timestamp).Before(*resp.Datapoints[j].Timestamp)
		})

		for _, v := range resp.Datapoints {
			// 提取不同的指标
			switch *s {
			case "Average":
				value = *v.Average
			case "Maximum":
				value = *v.Maximum
			case "Minimum":
				value = *v.Minimum
			case "Sum":
				value = *v.Sum
			case "SampleCount":
				value = *v.SampleCount
			default:
				if strings.Index(*s, "p") == 0 && v.ExtendedStatistics[*s] != nil {
					value = *v.ExtendedStatistics[*s]
				}
			}

			// terminate gap of data points
			timestamp := *v.Timestamp
			if _, ok := lastTimestamp[*s]; ok {
				nextTimestampFromLast := lastTimestamp[*s].Add(time.Duration(query.Period) * time.Second)
				// 如果存在GAP, 通过nil补充数据
				for timestamp.After(nextTimestampFromLast) {
					series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFromPtr(nil), float64(nextTimestampFromLast.Unix()*1000)))
					nextTimestampFromLast = nextTimestampFromLast.Add(time.Duration(query.Period) * time.Second)
				}
			}
			lastTimestamp[*s] = timestamp

			// 时间单位: ms
			series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFrom(value), float64(timestamp.Unix()*1000)))
		}

		// 添加一个时间序列数据
		queryRes.Series = append(queryRes.Series, &series)
	}

	return queryRes, nil
}

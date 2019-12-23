package bigquery

import (
	"context"
	"io/ioutil"
	"os"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQuery struct {
	c *bigquery.Client
}

func New(ctx context.Context, projectID string) (*BigQuery, error) {
	c, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		c: c,
	}, nil
}

func NewWithCredentials(ctx context.Context, credentials []byte, projectID string) (*BigQuery, error) {
	creds, err := google.CredentialsFromJSON(ctx, credentials, bigquery.Scope)
	if err != nil {
		return nil, err
	}

	c, err := bigquery.NewClient(ctx, projectID, option.WithCredentials(creds))
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		c: c,
	}, nil
}

func readCredentials(path string) ([]byte, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer fd.Close()

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func NewWithCredentialsPath(ctx context.Context, credentialsPath, projectID string) (*BigQuery, error) {
	credentials, err := readCredentials(credentialsPath)
	if err != nil {
		return nil, err
	}

	creds, err := google.CredentialsFromJSON(ctx, credentials, bigquery.Scope)
	if err != nil {
		return nil, err
	}

	c, err := bigquery.NewClient(ctx, projectID, option.WithCredentials(creds))
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		c: c,
	}, nil
}

func (b *BigQuery) Close() error {
	return b.c.Close()
}

func (b *BigQuery) Query(ctx context.Context, query string) (columns []string, contents []map[string]interface{}, err error) {
	schema, rows, err := b.QueryBase(ctx, query)
	if err != nil {
		return
	}

	columns = make([]string, 0, len(schema))
	for _, s := range schema {
		columns = append(columns, s.Name)
	}

	contents = make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		r := make(map[string]interface{}, len(columns))
		for index, column := range columns {
			r[column] = row[index]
		}

		contents = append(contents, r)
	}

	return
}

func (b *BigQuery) QueryBase(ctx context.Context, query string) (schema bigquery.Schema, rows [][]bigquery.Value, err error) {
	q := b.c.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return
	}

	rows = make([][]bigquery.Value, 0, it.TotalRows)
	for {
		var row []bigquery.Value

		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		rows = append(rows, row)
	}

	schema = it.Schema

	return
}

func (b *BigQuery) QueryDryRun(ctx context.Context, query string) (totalBytesProcessed int64, err error) {
	q := b.c.Query(query)

	q.DryRun = true

	j, err := q.Run(ctx)
	if err != nil {
		return
	}

	status := j.LastStatus()

	totalBytesProcessed = status.Statistics.TotalBytesProcessed

	return
}

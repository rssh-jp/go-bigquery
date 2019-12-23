package bigquery

import (
	"context"
	"testing"
)

const (
	credentialPath = "credentials.json"
	projectID      = "test-project"
	query          = "select name, age from `test-project.test_dataset.test_table` limit 10"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestSuccessDryRun(t *testing.T) {
	b, err := New(context.Background(), projectID)
	if err != nil {
		t.Error(err)
	}

	defer b.Close()

	totalBytes, err := b.QueryDryRun(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	t.Log("totalbytes: ", totalBytes)
}

func testSuccess(t *testing.T) {
	b, err := New(context.Background(), projectID)
	if err != nil {
		t.Error(err)
	}

	defer b.Close()

	columns, contents, err := b.Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	t.Log(columns)
	t.Log(contents)
}

func testSuccessWithCredentials(t *testing.T) {
	creds, err := readCredentials(credentialPath)
	if err != nil {
		t.Error(err)
	}

	b, err := NewWithCredentials(context.Background(), creds, projectID)
	if err != nil {
		t.Error(err)
	}

	defer b.Close()

	columns, contents, err := b.Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	t.Log(columns)
	t.Log(contents)
}

func testSuccessWithCredentialsPath(t *testing.T) {
	b, err := NewWithCredentialsPath(context.Background(), credentialPath, projectID)
	if err != nil {
		t.Error(err)
	}

	defer b.Close()

	columns, contents, err := b.Query(context.Background(), query)
	if err != nil {
		t.Error(err)
	}

	t.Log(columns)
	t.Log(contents)
}

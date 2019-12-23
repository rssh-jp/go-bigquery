# go-bigquery

# usage
``` main.go
package main

import(
    "context"
    "log"

    "github.com/rssh-jp/go-bigquery"
)

func main(){
    b, err := bigquery.New(context.Background(), "your-project-id")
    if err != nil {
        log.Fatal(err)
    }
    
    defer b.Close()
    
    columns, contents, err := b.Query(context.Background(), "your-select-query")
    if err != nil {
        log.Fatal(err)
    }
    
    log.Println(columns)
    log.Println(contents)
}
```


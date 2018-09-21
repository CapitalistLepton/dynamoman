package main 

import (
  "fmt"
  "os"
  "flag"
  "regexp"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {
  local := flag.Bool("l", false, "use local dynamodb instead of remote one")
  clear := flag.String("d", "", "name of table to clear data from")
  read := flag.String("o", "", "name of table to load data from")
  make := flag.String("w", "", "name of table to create backup from")
  makeAll := flag.Bool("a", false, "create backups of all tables")
  readAll := flag.Bool("u", false, "upload all the backups of all tables")
  copyFrom := flag.String("from", "", "stage to copy from")
  copyTo := flag.String("to", "", "stage to copy to")

  flag.Parse()
  noFlags := true

  var config *aws.Config
  if *local {
    config = &aws.Config{
      Region: aws.String("localhost"),
      Endpoint: aws.String("http://localhost:8000")}
  } else {
    config = &aws.Config{
      Region: aws.String("us-east-1")}
  }
  sess, err := session.NewSession(config)
  check(err)

  svc := dynamodb.New(sess)

  if len(*clear) > 0 { 
    clearTable(svc, *clear)
    noFlags = false
  }
  if len(*read) > 0 {
    load(svc, *read)
    noFlags = false
  }
  if len(*make) > 0 {
    backup(svc, *make)
    noFlags = false
  }
  if len(*copyFrom) > 0 && len(*copyTo) > 0 {
    copyStage(svc, *copyFrom, *copyTo)
    noFlags = false
  }
  if *makeAll {
    applyToTable(svc, backup)
    noFlags = false
  }
  if *readAll {
    applyToTable(svc, load)
    noFlags = false
  }
  if noFlags { 
    displayTables(svc)
  } 
}

func displayTables(svc *dynamodb.DynamoDB) {
  tables := listTables(svc)
  fmt.Println("Tables:")
  fmt.Println()
  for _, table := range tables {
    fmt.Println(*table)
  }
}

func applyToTable(svc *dynamodb.DynamoDB, fun func(*dynamodb.DynamoDB, string)) {
  tables := listTables(svc)
  for _, table := range tables {
    fun(svc, *table)
  }
}

func backup(svc *dynamodb.DynamoDB, name string) {
  file, err := os.Create(name + ".json")
  check(err)
  defer file.Close()
  backupTable(svc, name, file)
}

func load(svc *dynamodb.DynamoDB, name string) {
  file, err := os.Open(name + ".json")
  check(err)
  defer file.Close()
  loadBackup(svc, name, file)
}

func copyStage(svc *dynamodb.DynamoDB, from string, to string) {
  tables := listTables(svc)
  fromTables := filter(tables, from)
  re := regexp.MustCompile(from + "$")
  for _, table := range fromTables {
    new := re.ReplaceAllString(*table, to)
    copyFromTo(svc, *table, new)
  }
} 

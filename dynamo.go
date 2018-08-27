package main 

import (
  "fmt"
  "os"
  "io/ioutil"
  "encoding/json"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/service/dynamodb"
)

func listTables(db *dynamodb.DynamoDB) []*string {
  res, err := db.ListTables(&dynamodb.ListTablesInput{})
  if err != nil {
    fmt.Println(err)
    os.Exit(1)
  }
  return res.TableNames
}

func backupTable(db *dynamodb.DynamoDB, tableName string, file *os.File) {
  params := &dynamodb.ScanInput{
    TableName: aws.String(tableName)}
  err := db.ScanPages(params,
    func(page *dynamodb.ScanOutput, lastPage bool) bool {
      b, e := json.Marshal(page.Items)
      if e != nil {
        fmt.Println(e)
      } else { 
        _, err := file.Write(b) 
        check(err)
      }
      return lastPage
    })
  check(err)
}

func clearTable(db *dynamodb.DynamoDB, tableName string) {
  key := getKeyTable(db, tableName)
  params := &dynamodb.ScanInput{
    TableName: aws.String(tableName)}
  err := db.ScanPages(params,
    func(page *dynamodb.ScanOutput, lastPage bool) bool {
      for _, item := range page.Items {
        delKey := make(map[string]*dynamodb.AttributeValue)
        for _, k := range key {
          delKey[*k.AttributeName] = item[*k.AttributeName]
        }
        pars := &dynamodb.DeleteItemInput{
          Key: delKey,
          TableName: aws.String(tableName)}
        _, err := db.DeleteItem(pars)
        check(err)
      }
      return lastPage
    })
  check(err)
}

func loadBackup(db *dynamodb.DynamoDB, tableName string, file *os.File) {
  key := getKeyTable(db, tableName)
  bytes, err := ioutil.ReadAll(file)
  check(err)
  var items []map[string]*dynamodb.AttributeValue
  json.Unmarshal(bytes, &items)
  for _, item := range items {
    putKey := make(map[string]*dynamodb.AttributeValue)
    for _, k := range key {
      putKey[*k.AttributeName] = item[*k.AttributeName]
    }
    params := &dynamodb.PutItemInput{
      TableName: aws.String(tableName),
      Item: item}
    _, err := db.PutItem(params)
    check(err)
  }
}

func getKeyTable(db *dynamodb.DynamoDB, tableName string) []*dynamodb.KeySchemaElement { 
  params := &dynamodb.DescribeTableInput{
    TableName : aws.String(tableName)}
  res, err := db.DescribeTable(params)
  check(err)
  return res.Table.KeySchema
}

func check(e error) {
  if e != nil {
    fmt.Println(e)
    os.Exit(1)
  }
}

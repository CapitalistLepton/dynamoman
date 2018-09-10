package main 

import (
  "fmt"
  "os"
  "io/ioutil"
  "encoding/json"
  "regexp"

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

func copyFromTo(db *dynamodb.DynamoDB, from string, to string) {
  params := &dynamodb.ScanInput{
    TableName: aws.String(from),
  }
  stageRe := regexp.MustCompile(".*-")
  fromStage := stageRe.ReplaceAllString(from, "")
  toStage := stageRe.ReplaceAllString(to, "")
  re, err := regexp.Compile(fromStage)
  check(err)
  err = db.ScanPages(params,
    func(page *dynamodb.ScanOutput, lastPage bool) bool {
      for _, item := range page.Items {
        if item["thumbnail"] != nil {
          item["thumbnail"].S = aws.String(re.ReplaceAllString(*item["thumbnail"].S, toStage))
        }
        if item["results"] != nil {
          images := item["results"].L
          for _, img := range images {
            img.M["thumbnail"].S = aws.String(re.ReplaceAllString(*img.M["thumbnail"].S, toStage))
          }
        }
        pars := &dynamodb.PutItemInput{
          TableName: aws.String(to),
          Item: item,
        }
        _, e := db.PutItem(pars)
        check(e)
      }
      return lastPage
    })
  check(err)
}

func filter(slice []*string, postfix string) []*string {
  re := regexp.MustCompile(postfix + "$")
  var results []*string
  for _, str := range slice {
    if re.MatchString(*str) {
      results = append(results, str)
    } 
  }
  return results
}

func replace(slice []*string, orig string, repl string) []*string {
  re := regexp.MustCompile(orig)
  var results []*string
  for _, str := range slice {
    if re.MatchString(*str) {
       results = append(results, aws.String(re.ReplaceAllString(*str, repl)))
    }
  }
  return results
}

func check(e error) {
  if e != nil {
    fmt.Println(e)
    os.Exit(1)
  }
}

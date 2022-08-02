
import Foundation
import ClientRuntime
import AWSDynamoDB


struct AWSDynDB2Action {
    var text = "Hello, World!"

    

func getDynamoDbItem(dynamoDbClient: DynamoDbClient,
                     nameOfTable: String,
                     keyName: String,
                     keyVal: String) {
   
    let keyToGet = [keyName : DynamoDbClientTypes.AttributeValue.s(keyVal)]
        
    dynamoDbClient.getItem(input: GetItemInput(key: keyToGet, tableName: nameOfTable)) { result in
        switch(result) {
        case .success(let response):
            guard let numbersMap = response.item else {
                return
            }
            for returnedKey in numbersMap {
                print("\(returnedKey.key) : \(returnedKey.value)")
            }
        case .failure(let err):
            print(err)
        }
    }
}

    func describeDymamoDBTable(ddb: DynamoDbClient, nameOfTable: String?) {
        ddb.describeTable(input: DescribeTableInput(tableName: nameOfTable)) { result in
            switch(result) {
            case .success(let tableInfo):
                if let table = tableInfo.table,
                   let tableName = table.tableName,
                   let tableArn = table.tableArn,
                   let tableStatus = table.tableStatus {
                    print("Table name \(tableName)")
                    print("Table ARN:  \(tableArn)")
                    print("Table Status: \(tableStatus)")
                    print("Item count:  \(table.itemCount)")
                    print("Size (bytes): \(table.tableSizeBytes)")
                }        case .failure(let err):
                print(" NO GOOD ")
                print(err)
            }
        }
    }
    
    
    func basicScanDBTable(ddb: DynamoDbClient, nameOfTable: String? ) -> Dictionary<Date,String> {
        print (" SCAN TABLE ")
    
        var dateDict : [Date: String] = [:]
        var sortedKeys = dateDict.keys.sorted { $0 < $1 }
        
              ddb.scan( input: ScanInput( limit: 10000, tableName: "Entry"  )  )  { result in
                  switch(result) {
                    case .success(let response):
                           
                      guard let numbersMap = response.items else {
                          return
                      }
                
                      for returnedKey in numbersMap {
                    
                          dateDict[createDate(inputDate: valToString(inputAttributeValueString: returnedKey["date"]!))] = valToString(inputAttributeValueString:  returnedKey["entry"]!)
                    
                      }
                
                  case .failure(let err):
                      print(err)
                
                  }
         
                }
        

        return dateDict
    }
    
 
    
    func scanDBTable(ddb: DynamoDbClient, nameOfTable: String? ) -> Dictionary<Date,String> {
        print (" SCAN TABLE ")
    
        var scannedItems = 0
        var dateDict : [Date: String] = [:]
        var sortedKeys = dateDict.keys.sorted { $0 < $1 }
        
   //     var scanIn = ScanInput(attributesToGet: <#T##[String]?#>, conditionalOperator: <#T##DynamoDbClientTypes.ConditionalOperator?#>, consistentRead: <#T##Bool?#>, exclusiveStartKey: <#T##[String : DynamoDbClientTypes.AttributeValue]?#>, expressionAttributeNames: <#T##[String : String]?#>, expressionAttributeValues: <#T##[String : DynamoDbClientTypes.AttributeValue]?#>, filterExpression: <#T##String?#>, indexName: <#T##String?#>, limit: <#T##Int?#>, projectionExpression: <#T##String?#>, returnConsumedCapacity: <#T##DynamoDbClientTypes.ReturnConsumedCapacity?#>, scanFilter: <#T##[String : DynamoDbClientTypes.Condition]?#>, segment: <#T##Int?#>, select: <#T##DynamoDbClientTypes.Select?#>, tableName: <#T##String?#>, totalSegments: <#T##Int?#>)
        
        //var currentKey = ni
        var needAnotherScan = true
        var startEvaluatedKeyInt = Int(0) // [String:DynamoDbClientTypes.AttributeValue]()
        var startEvaluatedKeyString = String() // [String:DynamoDbClientTypes.AttributeValue]()
        var startEvaluatedKey = [String:DynamoDbClientTypes.AttributeValue]()
        
        while ( needAnotherScan )  {
            
            
            print ( " start kEY INT \( startEvaluatedKeyInt ) " )
            
            if ( startEvaluatedKeyInt == 0) {
            //if ( startEvaluatedKey["id"] == nil ) {
                ddb.scan( input: ScanInput( limit: 10000, tableName: "Entry"  )  )  { result in
        //    ddb.scan(input: ScanInput( )  { result in
            switch(result) {
            case .success(let response):
               
                print(" LastEvaluatedKey: \(response.lastEvaluatedKey) " )
                
                if (response.lastEvaluatedKey == nil) {
                    needAnotherScan = false
                }
                
                startEvaluatedKey = response.lastEvaluatedKey!
                
                guard let lastKey = response.lastEvaluatedKey else {
                    return
                }
                
                print(" start LastEvaluatedKey: \(startEvaluatedKey["id"]) " )
                print(" start lastKey: \(lastKey["id"]!) " )
                startEvaluatedKeyString = valToString(inputAttributeValueString: lastKey["id"]!)
                startEvaluatedKeyInt = valToInt(inputAttributeValue: lastKey["id"]!)
                print(" startEvaluatedKeyString: \(startEvaluatedKeyString) " )
                print(" startEvaluatedKeyInt: \(startEvaluatedKeyInt) " )
                
                
                guard let numbersMap = response.items else {
                    return
                }
                
                for returnedKey in numbersMap {
                    scannedItems += 1
                    dateDict[createDate(inputDate: valToString(inputAttributeValueString: returnedKey["date"]!))] = valToString(inputAttributeValueString:  returnedKey["entry"]!)
                    
                }
                
            case .failure(let err):
                print(err)
                
            }
            
            }
            } // end if isEmpty
            else {
                print(" RUNNING WITH START KEY \(startEvaluatedKey)" )
                
                    ddb.scan( input: ScanInput( exclusiveStartKey: startEvaluatedKey, limit: 10000, tableName: "Entry"  )  )  { result in
                        switch(result) {
                    case .success(let response):
                       
                        print(" RESPONSE LastEvaluatedKey: \( response.lastEvaluatedKey ) " )
                                            
                        guard let numbersMap = response.items else {
                            return
                        }
                        
                        for returnedKey in numbersMap {
                            scannedItems += 1
                            dateDict[createDate(inputDate: valToString(inputAttributeValueString: returnedKey["date"]!))] = valToString(inputAttributeValueString:  returnedKey["entry"]!)
                        }
                        
                        if (response.lastEvaluatedKey == nil) {
                            needAnotherScan = false
                        }
                        else {
                            startEvaluatedKey = response.lastEvaluatedKey!
                        }
                        
                    case .failure(let err):
                        
                        print(" FAILURE RESPONSE LastEvaluatedKey" )
                        
                        print(err)
                        
                    }
                    
                    
                }
        
        } // end else
            
        
        } // end of while
        
        print( " Items Scannned: \(scannedItems) ")
        return dateDict
    }
    
    func valToString(inputAttributeValueString: DynamoDbClientTypes.AttributeValue) -> String {
        //return "\(inputAttributeValue)"
        let value = String()
        guard case .s(let value) = inputAttributeValueString else {
               //     throw DecodingError.typeMismatch(String.self, .init(codingPath: self.codingPath, debugDescription: "Cannot convert from \(attribute)"))
            print(" Error converting attribute \(value) ")
            return ""
        }
        
        return value
    }
    
    func valToInt(inputAttributeValue: DynamoDbClientTypes.AttributeValue) -> Int {
        //return "\(inputAttributeValue)"
        let value = String()
        guard case .n(let value) = inputAttributeValue else {
               //     throw DecodingError.typeMismatch(String.self, .init(codingPath: self.codingPath, debugDescription: "Cannot convert from \(attribute)"))
            print(" Error converting attribute \(value) ")
            return 0
        }
        
        return Int(value)!
    }
    
    func createDate(inputDate: String) -> Date {
         let defaultDate = "2000-10-21T00:00:00Z"
         let dateFormatter = DateFormatter()
         dateFormatter.locale = Locale(identifier: "en_US_POSIX") // set locale to reliable US_POSIX
         dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
        
         //print (" inputDate in createDate: \(inputDate)")
         guard let localDate = dateFormatter.date(from:inputDate) else { return dateFormatter.date(from:defaultDate)! }
        
         return localDate
    }
    
    
    func listAllTables(ddb: DynamoDbClient) {
        print(" LIST TABLES ")
        ddb.listTables(input: ListTablesInput()) { result in
            switch(result) {
            case .success(let response):
                guard let namesOfTables = response.tableNames else {
                    return
                }
                for currName in namesOfTables {
                    print("Table name is \(currName)")
                }
            case .failure(let err):
                print(err)
            }
        }
    }

}

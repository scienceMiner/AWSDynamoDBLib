//
//  File.swift
//  
//
//  Created by Ethan Collopy on 29/06/2022.
//

import Foundation



public struct Entry {
 
    var iden: Int
    var fact: String
    var date: Date
    
    init(details: [String: Any]) {
        iden = details["_id"] as? Int ?? 0
        fact = details["text"] as? String ?? ""
        date = Date()
    }
    
    init(iden: Int, fact: String, date: Date ) {
        self.iden = iden
        self.fact = fact
        self.date = date
    }
    

}


public struct EntryMap {
    
    var entry: [Entry]
    
    public init(entryArray: [Entry] ) {
        entry = entryArray
    }
    
    enum CodingKeys: String, CodingKey {
        case entry = "entry"
        
    }
    
}

import Foundation
import Switchblade

public class AstraDBProvider: DataProvider {
    
    public var config: SwitchbladeConfig!
    public weak var blade: Switchblade!
    
    private var astraDBURL: URL
    private var astraDBToken: String
    private var decoder = JSONDecoder()
    private var encoder = JSONEncoder()
    private var ks: String
    
    public init(astraDBURL: String, astraDBToken: String, keyspace: String) {
        self.astraDBURL = URL(string: astraDBURL)!
        self.astraDBToken = astraDBToken
        self.ks = keyspace
        try? createTableAndKeyspaceIfNotExists()
    }
    
    private func createTableAndKeyspaceIfNotExists() throws {
        
        let cqlQuery = """
        CREATE TABLE IF NOT EXISTS \(ks).data (
            partition TEXT,
            area TEXT,
            id TEXT,
            value TEXT,                    -- JSON data should be stored as TEXT in Cassandra
            filter MAP<TEXT, TEXT>,
            updated TIMESTAMP,
            model TEXT,
            version INT,
            PRIMARY KEY ((partition, area), id)
        );
        """
        
        let success = executeCQL(cqlQuery)
        
        if success {
            print("Table 'data' created or already exists.")
        } else {
            throw NSError(domain: "AstraDBProvider", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "Failed to create the table."
            ])
        }
    }
    
    public func open() throws {
        // No operation needed for opening a connection to AstraDB.
    }
    
    public func close() throws {
        // No operation needed for closing a connection to AstraDB.
    }
    
    @discardableResult
    public func transact(_ mode: transaction) -> Bool {
        // Transactions are not supported directly by AstraDB REST API, return true to comply with protocol
        return true
    }
    
    @discardableResult
    public func put<T: Codable>(partition: String, key: String, keyspace: String, ttl: Int, filter: [String: String]?, _ object: T) -> Bool {
        do {
            
            let jsonString = String(data: try encoder.encode(object), encoding: .utf8)!
            
            var model: String? = nil
            var version: Int? = nil
            if let info = (T.self as? SchemaVersioned.Type)?.version {
                model = info.objectName
                version = info.version
            }
            
            let filterString = filter?.map { "'\($0.key)' : '\($0.value)'" }
                                       .joined(separator: ", ") ?? ""
            
            let ttlString = ttl > 0 ? " USING TTL \(ttl)" : ""
            
            let cqlQuery = """
            INSERT INTO \(ks).data (partition, area, id, value, filter, updated, model, version) VALUES ('\(partition)', '\(keyspace)', '\(key)', '\(jsonString)', {\(filterString)}, toTimestamp(now()), '\(model ?? "null")', \(version ?? 0)) \(ttlString);
            """
            
            return executeCQL(cqlQuery)
        } catch {
            print("Error encoding object: \(error)")
            return false
        }
    }
    
    @discardableResult
    public func delete(partition: String, key: String, keyspace: String) -> Bool {
        let cqlQuery = """
        DELETE FROM \(ks).data WHERE partition = '\(partition)' AND area = '\(keyspace)' AND id = '\(key)';
        """
        return executeCQL(cqlQuery)
    }
    
    @discardableResult
    public func get<T: Codable>(partition: String, key: String, keyspace: String) -> T? {
        do {
            
            let cqlQuery = """
            SELECT value FROM \(ks).data WHERE partition = '\(partition)' AND area = '\(keyspace)' AND id = '\(key)';
            """
            
            let result = queryCQL(cqlQuery) as [T]
            
            return result.first
        } catch {
            print("Error decoding response: \(error)")
            return nil
        }
    }
    
    @discardableResult
    public func query<T>(partition: String, keyspace: String, filter: [String : String]?, map: ((T) -> Bool)) -> [T] where T : Decodable, T : Encodable {
        var results: [T] = []
        
        for result: T in all(partition: partition, keyspace: keyspace, filter: filter) {
            if map(result) {
                results.append(result)
            }
        }
        
        return results
    }
    
    @discardableResult
    fileprivate func query<T: Codable>(partition: String, keyspace: String, filter: [String: String]?) -> [T] {
        
        var queryConditions = ""
        if let filter = filter {
            for (key, value) in filter {
                queryConditions += "AND filter['\(key)'] = '\(value)' "
            }
        }
        
        if queryConditions.isEmpty == false {
            queryConditions += " ALLOW FILTERING"
        }
        
        let cqlQuery = """
        SELECT value FROM \(ks).data WHERE partition = '\(partition)' AND area = '\(keyspace)' \(queryConditions);
        """
        
        let results: [T] = queryCQL(cqlQuery)
        return results
        
    }
    
    @discardableResult
    public func all<T: Codable>(partition: String, keyspace: String, filter: [String: String]?) -> [T] {
        return query(partition: partition, keyspace: keyspace, filter: filter)
    }
    
    public func iterate<T: Codable>(partition: String, keyspace: String, filter: [String: String]?, iterator: ((T) -> Void)) {
        let results: [T] = all(partition: partition, keyspace: keyspace, filter: filter)
        for result in results {
            iterator(result)
        }
    }
    
    public func migrate<FromType: SchemaVersioned, ToType: SchemaVersioned>(from: FromType.Type, to: ToType.Type, migration: @escaping ((FromType) -> ToType?)) {
        self.migrate(iterator: migration)
    }
    
    fileprivate func migrate<T: SchemaVersioned>(iterator: @escaping ((T) -> SchemaVersioned?)) {
        let fromInfo = T.version
        
        // CQL query to select all entries of the specified model and version
        let cqlQuery = """
        SELECT partition, area, id, value FROM \(ks).data WHERE model = '\(fromInfo.objectName)' AND version = \(fromInfo.version) ALLOW FILTERING;
        """
        
        // Perform the query
        let results: [[String: String]] = rowFromCQL(cqlQuery)
        
        // Iterate over each row in the result set
        for row in results {
            if let partition = row["partition"],
               let area = row["area"],
               let id = row["id"],
               let value = row["value"] {
                
                // Decode the value field to the original object
                if let data = value.data(using: .utf8),
                   let object = try? decoder.decode(T.self, from: data) {
                    
                    // Apply the migration function
                    if let newObject = iterator(object) {
                        
                        // Handle filterable objects
                        var filters: [String: String] = [:]
                        if let filterable = newObject as? Filterable {
                            filters = filterable.filters.dictionary
                        }
                        
                        // Save the new object in AstraDB
                        let _ = self.put(partition: partition, key: id, keyspace: area, ttl: -1, filter: filters, newObject)
                    } else {
                        // If migration returns nil, delete the object
                        let _ = self.delete(partition: partition, key: id, keyspace: area)
                    }
                }
            }
        }
    }

    
    @discardableResult
    public func ids(partition: String, keyspace: String, filter: [String: String]?) -> [String] {
        do {
        
            var queryConditions = ""
            if let filter = filter {
                for (key, value) in filter {
                    queryConditions += "AND filter['\(key)'] = '\(value)' "
                }
            }
            
            if queryConditions.isEmpty == false {
                queryConditions += " ALLOW FILTERING"
            }
            
            let cqlQuery = """
            SELECT id FROM \(ks).data WHERE partition = '\(partition)' AND area = '\(keyspace)' \(queryConditions);
            """
            
            
            let result: [String] = queryCQL(cqlQuery)
            return result
            
        } catch {
            print("Error executing ids query: \(error)")
            return []
        }
    }
    
    public func removeAllRecords(partition: String, keyspace: String) -> Bool {
        do {
            
            let cqlQuery = """
            DELETE FROM \(ks).data WHERE partition = '\(partition)' AND area = '\(keyspace)';
            """
            
            return executeCQL(cqlQuery)
        } catch {
            print("Error executing removeAllRecords: \(error)")
            return false
        }
    }
    
    public func truncateTable() {
        let _ = executeCQL("TRUNCATE \(ks).data;")
    }
    
    private func executeCQL(_ cqlQuery: String) -> Bool {
        var request = URLRequest(url: astraDBURL)
        request.httpMethod = "POST"
        request.addValue("text/plain", forHTTPHeaderField: "Content-Type")
        request.addValue(astraDBToken, forHTTPHeaderField: "x-cassandra-token")
        request.httpBody = cqlQuery.data(using: .utf8)
        
        let (data, response, error) = URLSession.shared.synchronousDataTask(with: request)
        
        if let error = error {
            print("Error executing CQL: \(error)")
            return false
        }
        
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode <= 299 else {
            print("CQL request failed with response: \(String(describing: response))")
            if let data = data {
                print(String(data: data, encoding: .utf8) ?? "")
            }
            return false
        }
        
        return true
    }
    
    private func rowFromCQL(_ cqlQuery: String) -> [[String:String]] {
        var request = URLRequest(url: astraDBURL)
        request.httpMethod = "POST"
        request.addValue("text/plain", forHTTPHeaderField: "Content-Type")
        request.addValue(astraDBToken, forHTTPHeaderField: "x-cassandra-token")
        request.httpBody = cqlQuery.data(using: .utf8)
        
        let (data, response, error) = URLSession.shared.synchronousDataTask(with: request)
        
        if let error = error {
            print("Error executing CQL: \(error)")
            return []
        }
        
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode <= 299, let data = data else {
            print("CQL request failed with response: \(String(describing: response))")
            if let data = data {
                print(String(data: data, encoding: .utf8) ?? "")
            }
            return []
        }
        
        var results: [[String:String]] = []
        
        do {
            let jsonResponse = try JSONSerialization.jsonObject(with: data, options: [])
            if let result = jsonResponse as? [String: Any], let count = result["count"] as? Int {
                if count > 0 {
                    if let data = result["data"] as? [[String:String]] {
                        for row in data {
                            results.append(row)
                        }
                    }
                }
            }
            return results
        } catch {
            print("Error parsing JSON response: \(error)")
            print("Error parsing JSON objcet: \([String:String].self)")
            return results
        }
    }
    
    let lock = Mutex()
    var queriesLog: [String:Int] = [:]
    
    private func queryCQL<T: Codable>(_ cqlQuery: String) -> [T] {
        var request = URLRequest(url: astraDBURL)
        request.httpMethod = "POST"
        request.addValue("text/plain", forHTTPHeaderField: "Content-Type")
        request.addValue(astraDBToken, forHTTPHeaderField: "x-cassandra-token")
        request.httpBody = cqlQuery.data(using: .utf8)
        
        let (data, response, error) = URLSession.shared.synchronousDataTask(with: request)
        
        lock.mutex {
            if let current = queriesLog[cqlQuery] {
                queriesLog[cqlQuery] = (current + 1)
            } else {
                queriesLog[cqlQuery] = 1
            }
            print(queriesLog)
        }
        
        if let error = error {
            print("Error executing CQL: \(error)")
            return []
        }
        
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode <= 299, let data = data else {
            print("CQL request failed with response: \(String(describing: response))")
            if let data = data {
                print(String(data: data, encoding: .utf8) ?? "")
            }
            return []
        }
        
        var results: [T] = []
        
        do {
            let jsonResponse = try JSONSerialization.jsonObject(with: data, options: [])
            if let result = jsonResponse as? [String: Any], let count = result["count"] as? Int {
                if count > 0 {
                    if let data = result["data"] as? [[String:String]] {
                        for objectString in data.compactMap({ $0["value"] ?? nil}) {
                            let objectData = Data(objectString.utf8)
                            let object = try decoder.decode(T.self, from: objectData)
                            results.append(object)
                        }
                    }
                }
            }
            return results
        } catch {
            print("Error parsing JSON response: \(error)")
            return results
        }
    }
}

// Helper extension to perform synchronous network requests
extension URLSession {
    func synchronousDataTask(with request: URLRequest) -> (Data?, URLResponse?, Error?) {
        var data: Data?
        var response: URLResponse?
        var error: Error?
        
        let semaphore = DispatchSemaphore(value: 0)
        
        let task = self.dataTask(with: request) {
            data = $0
            response = $1
            error = $2
            semaphore.signal()
        }
        
        task.resume()
        semaphore.wait()
        
        return (data, response, error)
    }
}

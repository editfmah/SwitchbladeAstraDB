import XCTest
@testable import SwitchbladeAstraDB
final class SwitchbladePostgresTests: XCTestCase {
    
    static var allTests = [
        ("testPersistObject",testPersistObject),
        ("testPersistQueryObject",testPersistQueryObject),
        ("testPersistMultipleObjectsAndCheckAll", testPersistMultipleObjectsAndCheckAll),
        ("testPersistMultipleObjectsAndFilterAll",testPersistMultipleObjectsAndFilterAll),
        ("testPersistMultipleObjectsAndQuery",testPersistMultipleObjectsAndQuery),
        ("testPersistMultipleObjectsAndQueryMultipleParams", testPersistMultipleObjectsAndQueryMultipleParams),
    ]
    
}

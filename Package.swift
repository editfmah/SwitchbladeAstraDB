// swift-tools-version:5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwitchbladeAstraDB",
    platforms: [
        .macOS(.v10_15)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "SwitchbladeAstraDB",
            targets: ["SwitchbladeAstraDB"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/editfmah/switchblade.git", from: "0.0.8"),
    ],
    targets: [
        .target(
            name: "SwitchbladeAstraDB",
            dependencies: [.product(name: "Switchblade", package: "switchblade")]),
        .testTarget(
            name: "SwitchbladeAstraDBTests",
            dependencies: ["SwitchbladeAstraDB"]),
    ]
)

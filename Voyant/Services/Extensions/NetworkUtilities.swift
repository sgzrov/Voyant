//
//  NetworkUtilities.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

struct APIConstants {
    static let baseURL = "https://healthpredictor-production.up.railway.app"
}

enum NetworkError: Error {
    case invalidURL
    case invalidResponse
    case decodingError
    case uploadFailed
    case fileNotFound
    case streamingError(String)
    case authenticationFailed
}

enum MultipartField {
    case file(name: String, filename: String, contentType: String, data: Data)
    case text(name: String, value: String)
}

struct MultipartFormBuilder {
    static func buildMultipartForm(fields: [MultipartField], boundary: String) -> Data {
        var body = Data()
        for field in fields {
            appendMultipartField(to: &body, field: field, boundary: boundary)
        }
        body.append("--\(boundary)--\r\n")
        return body
    }

    private static func appendMultipartField(to body: inout Data, field: MultipartField, boundary: String) {
        body.append("--\(boundary)\r\n")

        switch field {
        case .file(let name, let filename, let contentType, let data):
            body.append("Content-Disposition: form-data; name=\"\(name)\"; filename=\"\(filename)\"\r\n")
            body.append("Content-Type: \(contentType)\r\n\r\n")
            body.append(data)
            body.append("\r\n")
        case .text(let name, let value):
            body.append("Content-Disposition: form-data; name=\"\(name)\"\r\n\r\n")
            body.append("\(value)\r\n")
        }
    }
}

struct FileUtilities {
    static func readFileData(from path: String) throws -> Data {
        guard FileManager.default.fileExists(atPath: path) else {
            throw NetworkError.fileNotFound
        }

        let fileURL = URL(fileURLWithPath: path)
        return try Data(contentsOf: fileURL)
    }

    static func readFileData(from fileURL: URL) async throws -> Data {
        var didStartAccessing = false

        if fileURL.startAccessingSecurityScopedResource() {
            didStartAccessing = true
            print("Accessed file.")
        } else {
            print("Could not access file.")
        }

        defer {
            if didStartAccessing {
                fileURL.stopAccessingSecurityScopedResource()
                print("Stopped accessing file.")
            }
        }

        return try Data(contentsOf: fileURL)
    }
}

extension Data {
    mutating func append(_ string: String) {
        append(Data(string.utf8))
    }
}
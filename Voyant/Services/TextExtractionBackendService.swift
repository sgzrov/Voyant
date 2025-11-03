//
//  TextExtractionBackendService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

class TextExtractionBackendService: TextExtractionBackendServiceProtocol {

    static let shared = TextExtractionBackendService()

    private let authService: AuthServiceProtocol

    private init() {
        self.authService = AuthService.shared
    }

    func extractTextFromFile(fileURL: URL) async throws -> String {
        let fileData = try await readFileDataFromURL(fileURL)
        return try await performTextExtraction(fileData: fileData, filename: fileURL.lastPathComponent)
    }

    func extractTextFromURL(urlString: String) async throws -> String {
        return try await performTextExtraction(urlString: urlString)
    }

    private func readFileDataFromURL(_ fileURL: URL) async throws -> Data {
        return try await FileUtilities.readFileData(from: fileURL)
    }

    private func performTextExtraction(fileData: Data? = nil, filename: String? = nil, urlString: String? = nil) async throws -> String {
        let endpoint = "/extract-text/"

        var fields: [MultipartField] = []

        if let fileData = fileData, let filename = filename {
            fields.append(.file(name: "file", filename: filename, contentType: "application/pdf", data: fileData))
        } else if let urlString = urlString {
            fields.append(.text(name: "url", value: urlString))
        }

        let boundary = UUID().uuidString
        let body = MultipartFormBuilder.buildMultipartForm(fields: fields, boundary: boundary)

        var request = try await authService.authenticatedRequest(
            for: endpoint,
            method: "POST",
            body: body
        )
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.timeoutInterval = 120

        let (data, response) = try await URLSession.shared.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 else {
            throw NetworkError.invalidResponse
        }
        guard let text = String(data: data, encoding: .utf8) else {
            throw NetworkError.decodingError
        }
        return text
    }
}


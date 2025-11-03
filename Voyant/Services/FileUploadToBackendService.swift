//
//  FileUploadToBackendService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

struct UploadResponse: Codable {
    let s3Url: String
    let message: String

    enum CodingKeys: String, CodingKey {
        case s3Url = "s3_url"
        case message
    }
}

class FileUploadToBackendService: FileUploadToBackendServiceProtocol {

    static let shared = FileUploadToBackendService()

    private let authService: AuthServiceProtocol

    private init() {
        self.authService = AuthService.shared
    }

    func uploadHealthDataFile(fileData: Data) async throws -> String {
        print("🔍 UPLOAD: uploadHealthDataFile called")
        print("🔍 UPLOAD: File data size: \(fileData.count) bytes")

        let fields: [MultipartField] = [
            .file(name: "file", filename: "user_health_data.csv", contentType: "text/csv", data: fileData)
        ]
        print("🔍 UPLOAD: Created multipart fields")

        let boundary = UUID().uuidString
        print("🔍 UPLOAD: Generated boundary: \(boundary)")

        let body = MultipartFormBuilder.buildMultipartForm(fields: fields, boundary: boundary)
        print("🔍 UPLOAD: Built multipart form, body size: \(body.count) bytes")

        print("🔍 UPLOAD: Creating authenticated request")
        var request = try await authService.authenticatedRequest(
            for: "/files/upload-health-data/",
            method: "POST",
            body: body
        )
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")

        print("🔍 UPLOAD: Request prepared with \(body.count) bytes")
        print("🔍 UPLOAD: Request headers: \(request.allHTTPHeaderFields ?? [:])")
        print("🔍 UPLOAD: Request URL: \(request.url?.absoluteString ?? "nil")")

        print("🔍 UPLOAD: Starting data task")
        let (data, response) = try await URLSession.shared.data(for: request)
        print("🔍 UPLOAD: Data task completed")

        if let httpResponse = response as? HTTPURLResponse {
            print("🔍 UPLOAD: Response status: \(httpResponse.statusCode)")
            print("🔍 UPLOAD: Response headers: \(httpResponse.allHeaderFields)")
        }

        guard let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 else {
            print("🔍 UPLOAD: Upload failed with status: \((response as? HTTPURLResponse)?.statusCode ?? -1)")
            print("🔍 UPLOAD: Response data: \(String(data: data, encoding: .utf8) ?? "nil")")
            throw NetworkError.uploadFailed
        }

        print("🔍 UPLOAD: Upload successful, decoding response")
        do {
            let uploadResponse = try JSONDecoder().decode(UploadResponse.self, from: data)
            print("🔍 UPLOAD: Successfully uploaded, S3 URL: \(uploadResponse.s3Url)")
            return uploadResponse.s3Url
        } catch {
            print("🔍 UPLOAD: Decoding error: \(error)")
            print("🔍 UPLOAD: Response data: \(String(data: data, encoding: .utf8) ?? "nil")")
            throw NetworkError.decodingError
        }
    }

    func buildMultipartRequest(endpoint: String, fileData: Data, additionalFields: [String: String] = [:]) async throws -> URLRequest {
        var fields: [MultipartField] = [
            .file(name: "file", filename: "health_data.csv", contentType: "text/csv", data: fileData)
        ]

        // Add additional text fields
        for (name, value) in additionalFields {
            fields.append(.text(name: name, value: value))
        }

        let boundary = UUID().uuidString
        let body = MultipartFormBuilder.buildMultipartForm(fields: fields, boundary: boundary)

        var request = try await authService.authenticatedRequest(
            for: endpoint,
            method: "POST",
            body: body
        )
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")

        return request
    }
}
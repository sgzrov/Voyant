//
//  AgentBackendService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

class AgentBackendService: AgentBackendServiceProtocol {

    static let shared = AgentBackendService()

    private let authService: AuthServiceProtocol
    private let sseService: SSEServiceProtocol
    private let userFileCacheService: UserFileCacheServiceProtocol
    private let fileUploadService: FileUploadToBackendServiceProtocol

    private init() {
        self.authService = AuthService.shared
        self.sseService = SSEService.shared
        self.userFileCacheService = UserFileCacheService.shared
        self.fileUploadService = FileUploadToBackendService.shared
    }

    func chatWithCIStream(csvFilePath: String, userInput: String, conversationId: String?) async throws -> AsyncStream<String> {
        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: csvFilePath) else {
            throw NetworkError.fileNotFound
        }
        guard let csvData = fileManager.contents(atPath: csvFilePath) else {
            throw NetworkError.fileNotFound
        }
        let s3Url = try await fileUploadService.uploadHealthDataFile(fileData: csvData)

        var body: [String: Any] = ["s3_url": s3Url, "user_input": userInput]

        if let conversationId = conversationId {
            body["conversation_id"] = conversationId
        }

        let jsonData = try JSONSerialization.data(withJSONObject: body)
        var request = try await authService.authenticatedRequest(
            for: "/chat-with-ci/",
            method: "POST",
            body: jsonData
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await sseService.streamSSE(request: request)
    }
}
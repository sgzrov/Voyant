//
//  AgentBackendService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

struct CodeInterpreterResponse: Codable {
    let useCodeInterpreter: Bool

    enum CodingKeys: String, CodingKey {
        case useCodeInterpreter = "use_code_interpreter"
    }
}


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

    func shouldUseCodeInterpreter(userInput: String) async throws -> Bool {
        let jsonData = try JSONSerialization.data(withJSONObject: ["user_input": userInput])
        let request = try await authService.authenticatedRequest(
            for: "/should-use-code-interpreter/",
            method: "POST",
            body: jsonData
        )

        let (data, _) = try await URLSession.shared.data(for: request)
        do {
            let response = try JSONDecoder().decode(CodeInterpreterResponse.self, from: data)
            return response.useCodeInterpreter
        } catch {
            throw NetworkError.decodingError
        }
    }

    func simpleChatStream(userInput: String, conversationId: String?) async throws -> AsyncStream<String> {
        var body: [String: Any] = ["user_input": userInput]

        if let conversationId = conversationId {
            body["conversation_id"] = conversationId
        }

        let jsonData = try JSONSerialization.data(withJSONObject: body)
        var request = try await authService.authenticatedRequest(
            for: "/simple-chat/",
            method: "POST",
            body: jsonData
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await sseService.streamSSE(request: request)
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

    // Removed study-related endpoints
}
//
//  AgentBackendService.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation

class AgentBackendService {

    static let shared = AgentBackendService()

    private let authService: AuthService
    private let sseService: SSEService

    private init() {
        self.authService = AuthService.shared
        self.sseService = SSEService.shared
    }

    func chatStream(userInput: String, conversationId: String?) async throws -> AsyncStream<String> {
        var body: [String: Any] = ["user_input": userInput]
        if let conversationId = conversationId {
            body["conversation_id"] = conversationId
        }
        let jsonData = try JSONSerialization.data(withJSONObject: body)
        var request = try await authService.authenticatedRequest(
            for: "/chat/stream/",
            method: "POST",
            body: jsonData
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await sseService.streamSSE(request: request)
    }
}
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

    func chatStream(userInput: String, conversationId: String?, provider: String?, model: String?) async throws -> AsyncStream<String> {
        var body: [String: Any] = ["question": userInput]
        if let conversationId = conversationId {
            body["conversation_id"] = conversationId
        }
        if let provider = provider { body["provider"] = provider }
        if let model = model { body["model"] = model }
        let jsonData = try JSONSerialization.data(withJSONObject: body)
        var request = try await authService.authenticatedRequest(
            for: "/chat/tool-sql/stream",
            method: "POST",
            body: jsonData
        )
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await sseService.streamSSE(request: request)
    }

    func uploadHealthCSV(_ data: Data, uploadMode: String? = nil) async throws -> (String) {
        var request = try await authService.authenticatedRequest(for: "/health/upload-csv", method: "POST")
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        if let uploadMode = uploadMode {
            request.setValue(uploadMode, forHTTPHeaderField: "x-upload-mode")
        }

        var body = Data()
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"health.csv\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: text/csv\r\n\r\n".data(using: .utf8)!)
        body.append(data)
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)
        request.httpBody = body

        let (respData, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw NSError(domain: "upload", code: -1, userInfo: [NSLocalizedDescriptionKey: "Upload failed"])
        }
        struct UploadResponse: Decodable {
            let task_id: String?
            let status: String?
            let message: String?
        }
        let uploadResp = try? JSONDecoder().decode(UploadResponse.self, from: respData)

        // Log upload status for debugging
        if let status = uploadResp?.status, let message = uploadResp?.message {
            print("[Upload] Status: \(status), Message: \(message)")
        }

        return uploadResp?.task_id ?? ""
    }

    struct HealthTaskStatus: Decodable {
        let id: String
        let state: String
    }

    func getHealthTaskStatus(_ taskId: String) async throws -> HealthTaskStatus {
        let request = try await authService.authenticatedRequest(for: "/health/task-status/\(taskId)", method: "GET")
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw NSError(domain: "upload", code: -2, userInfo: [NSLocalizedDescriptionKey: "Status check failed"])
        }
        return try JSONDecoder().decode(HealthTaskStatus.self, from: data)
    }

    func waitForHealthTask(_ taskId: String, timeout: TimeInterval = 120, pollInterval: TimeInterval = 2) async throws -> HealthTaskStatus {
        let start = Date()
        while Date().timeIntervalSince(start) < timeout {
            let st = try await getHealthTaskStatus(taskId)
            if st.state == "SUCCESS" || st.state == "FAILURE" || st.state == "REVOKED" {
                return st
            }
            try await Task.sleep(nanoseconds: UInt64(pollInterval * 1_000_000_000))
        }
        throw NSError(domain: "upload", code: -3, userInfo: [NSLocalizedDescriptionKey: "Status timeout"])
    }

    func healthQueryStream(question: String, conversationId: String? = nil, provider: String? = nil, model: String? = nil, decisionModel: String? = nil) async throws -> AsyncStream<String> {
        var payload: [String: Any] = ["question": question]
        if let conversationId = conversationId { payload["conversation_id"] = conversationId }
        if let provider = provider { payload["provider"] = provider }
        if let model = model { payload["model"] = model }
        if let decisionModel = decisionModel { payload["decision_model"] = decisionModel }
        let body = try JSONSerialization.data(withJSONObject: payload)
        var request = try await authService.authenticatedRequest(for: "/chat/tool-sql/stream", method: "POST", body: body)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        return try await sseService.streamSSE(request: request)
    }

    // MARK: - Overview
    func getHealthOverview() async throws -> [String: Any] {
        let request = try await authService.authenticatedRequest(for: "/health/overview", method: "GET")
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw NSError(domain: "overview", code: -1, userInfo: [NSLocalizedDescriptionKey: "Overview fetch failed"])
        }
        let obj = try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        return obj ?? [:]
    }

    func refreshHealthOverview() async throws -> [String: Any] {
        let request = try await authService.authenticatedRequest(for: "/health/overview/refresh", method: "POST")
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw NSError(domain: "overview", code: -2, userInfo: [NSLocalizedDescriptionKey: "Overview refresh failed"])
        }
        let obj = try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        return obj ?? [:]
    }
}
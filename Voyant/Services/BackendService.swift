//
//  BackendService.swift
//  HealthPredictor
//
//  Created by Stephan  on 22.06.2025.
//

import Foundation
import Combine

struct SessionDTO: Decodable {
    let conversationId: String
    let lastActiveDate: String?

    enum CodingKeys: String, CodingKey {
        case conversationId = "conversation_id"
        case lastActiveDate = "last_active_date"
    }
}

struct ChatSessionsResponse: Decodable {
    let sessions: [SessionDTO]
}

class BackendService {

    static let shared = BackendService()

    private let agentService: AgentBackendServiceProtocol
    private let textExtractionService: TextExtractionBackendServiceProtocol
    private let fileUploadService: FileUploadToBackendServiceProtocol

    private init() {
        self.agentService = AgentBackendService.shared
        self.textExtractionService = TextExtractionBackendService.shared
        self.fileUploadService = FileUploadToBackendService.shared
    }

    func chatWithCI(csvFilePath: String, userInput: String, conversationId: String? = nil) async throws -> AsyncStream<String> {
        return try await agentService.chatWithCIStream(csvFilePath: csvFilePath, userInput: userInput, conversationId: conversationId)
    }

    func simpleChat(userInput: String, conversationId: String? = nil) async throws -> AsyncStream<String> {
        return try await agentService.simpleChatStream(userInput: userInput, conversationId: conversationId)
    }

    func shouldUseCodeInterpreter(userInput: String) async throws -> Bool {
        return try await agentService.shouldUseCodeInterpreter(userInput: userInput)
    }

    func extractTextFromFile(fileURL: URL) async throws -> String {
        return try await textExtractionService.extractTextFromFile(fileURL: fileURL)
    }

    func extractTextFromURL(urlString: String) async throws -> String {
        return try await textExtractionService.extractTextFromURL(urlString: urlString)
    }

    func uploadHealthDataFile(fileData: Data) async throws -> String {
        return try await fileUploadService.uploadHealthDataFile(fileData: fileData)
    }
}

extension BackendService {

    private func makeAuthenticatedRequest(url: URL, userToken: String) -> URLRequest {
        var request = URLRequest(url: url)
        request.setValue("Bearer \(userToken)", forHTTPHeaderField: "Authorization")
        return request
    }

    private func handleTokenRefresh<T>(_ operation: @escaping (String) async throws -> T) async throws -> T {
        let freshToken = try await TokenManager.shared.forceRefreshToken()
        return try await operation(freshToken)
    }

    // MARK: - Chat Operations

    func fetchChatSessions(userToken: String, completion: @escaping ([(String, Date?)]) -> Void) {
        guard let url = URL(string: "\(APIConstants.baseURL)/chat/retrieve-chat-sessions/") else {
            DispatchQueue.main.async { completion([]) }
            return
        }

        let request = makeAuthenticatedRequest(url: url, userToken: userToken)
        URLSession.shared.dataTask(with: request) { data, response, error in
            if let error = error {
                print("Error fetching chat sessions: \(error)")
                DispatchQueue.main.async { completion([]) }
                return
            }

            guard let httpResponse = response as? HTTPURLResponse else {
                DispatchQueue.main.async { completion([]) }
                return
            }

            if httpResponse.statusCode == 401 {
                Task {
                    do {
                        let freshToken = try await TokenManager.shared.forceRefreshToken()
                        self.fetchChatSessions(userToken: freshToken, completion: completion)
                    } catch {
                        DispatchQueue.main.async { completion([]) }
                    }
                }
                return
            }

            guard httpResponse.statusCode == 200, let data = data else {
                DispatchQueue.main.async { completion([]) }
                return
            }

            guard let result = try? JSONDecoder().decode(ChatSessionsResponse.self, from: data) else {
                DispatchQueue.main.async { completion([]) }
                return
            }

            let sessions = result.sessions.map { session in
                (session.conversationId, DateUtilities.parseBackendDate(session.lastActiveDate))
            }
            DispatchQueue.main.async { completion(sessions) }
        }.resume()
    }

    func fetchChatHistory(conversationId: String, userToken: String, completion: @escaping ([ChatMessage]) -> Void) {
        guard let url = URL(string: "\(APIConstants.baseURL)/chat/all-messages/\(conversationId)") else {
            DispatchQueue.main.async { completion([]) }
            return
        }

        let request = makeAuthenticatedRequest(url: url, userToken: userToken)
        let decoder = DateUtilities.createBackendDecoder()
        URLSession.shared.dataTask(with: request) { data, response, error in
            if let error = error {
                print("Error fetching chat history: \(error)")
                DispatchQueue.main.async { completion([]) }
                return
            }

            guard let httpResponse = response as? HTTPURLResponse else {
                DispatchQueue.main.async { completion([]) }
                return
            }

            if httpResponse.statusCode == 401 {
                Task {
                    do {
                        let freshToken = try await TokenManager.shared.forceRefreshToken()
                        self.fetchChatHistory(conversationId: conversationId, userToken: freshToken, completion: completion)
                    } catch {
                        DispatchQueue.main.async { completion([]) }
                    }
                }
                return
            }

            guard httpResponse.statusCode == 200,
                  let data = data,
                  let messages = try? decoder.decode([ChatMessage].self, from: data) else {
                DispatchQueue.main.async { completion([]) }
                return
            }

            DispatchQueue.main.async { completion(messages) }
        }.resume()
    }

    // Removed study-related operations
}

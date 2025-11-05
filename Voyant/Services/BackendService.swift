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

    private let agentService: AgentBackendService

    private init() {
        self.agentService = AgentBackendService.shared
    }

    func chat(userInput: String, conversationId: String? = nil, provider: String? = nil, model: String? = nil) async throws -> AsyncStream<String> {
        return try await agentService.chatStream(userInput: userInput, conversationId: conversationId, provider: provider, model: model)
    }
}

extension BackendService {

    private func makeAuthenticatedRequest(url: URL, userToken: String) -> URLRequest {
        var request = URLRequest(url: url)
        request.setValue("Bearer \(userToken)", forHTTPHeaderField: "Authorization")
        return request
    }

    private func handleTokenRefresh<T>(_ operation: @escaping (String) async throws -> T) async throws -> T {
        let freshToken = try await AuthService.forceRefreshToken()
        return try await operation(freshToken)
    }

    // MARK: - Chat Operations

    func fetchChatSessions(userToken: String, completion: @escaping ([(String, Date?)]) -> Void) {
        guard let url = URL(string: "\(AuthService.backendBaseURL)/chat/retrieve-chat-sessions/") else {
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
                        let freshToken = try await AuthService.forceRefreshToken()
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
                (session.conversationId, Self.parseBackendDate(session.lastActiveDate))
            }
            DispatchQueue.main.async { completion(sessions) }
        }.resume()
    }

    func fetchChatHistory(conversationId: String, userToken: String, completion: @escaping ([ChatMessage]) -> Void) {
        guard let url = URL(string: "\(AuthService.backendBaseURL)/chat/all-messages/\(conversationId)") else {
            DispatchQueue.main.async { completion([]) }
            return
        }

        let request = makeAuthenticatedRequest(url: url, userToken: userToken)
        let decoder = Self.createBackendDecoder()
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
                        let freshToken = try await AuthService.forceRefreshToken()
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

    // MARK: - (Studies removed)
}

// MARK: - Inlined Date Utilities
extension BackendService {
    private static func parseBackendDate(_ dateString: String?) -> Date? {
        guard let dateString = dateString else { return nil }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZZZZ"
        return formatter.date(from: dateString)
    }

    private static func createBackendDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let dateString = try container.decode(String.self)
            guard let date = parseBackendDate(dateString) else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Cannot decode date string \(dateString)"
                )
            }
            return date
        }
        return decoder
    }
}

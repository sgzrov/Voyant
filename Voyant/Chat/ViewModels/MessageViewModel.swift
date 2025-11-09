//
//  MessageViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 18.06.2025.
//

import Foundation
import SwiftUI

@MainActor
class MessageViewModel: ObservableObject {

    @Published var isLoading: Bool = false
    @Published var inputMessage: String = ""
    @Published var messages: [ChatMessage]
    @Published var selectedModel: ModelOption = .openai_gpt5mini

    private var session: ChatSession

    private let backendService = BackendService.shared

    private let userToken: String

    init(session: ChatSession, userToken: String) {
        self.session = session
        self.userToken = userToken
        self.messages = session.messages
    }

    func sendMessage() {
        guard !inputMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !isLoading else { return }

        let userMessage = ChatMessage(content: inputMessage, role: .user)
        messages.append(userMessage)
        session.messages = messages

        let userInput = inputMessage
        inputMessage = ""
        isLoading = true

        Task {
            try? await Task.sleep(nanoseconds: 500_000_000)
            let thinkingMessage = ChatMessage(content: "", role: .assistant, state: .streaming)
            messages.append(thinkingMessage)
            session.messages = messages

            await processMessage(userInput: userInput)
        }
    }

    private func processMessage(userInput: String) async {
        do {
            let stream = try await backendService.chat(
                userInput: userInput,
                conversationId: session.conversationId,
                provider: selectedModel.providerId,
                model: selectedModel.modelId
            )

            let messageIndex = messages.count - 1
            var isFirstChunk = true
            var fullContent = ""

            for await chunk in stream {
                if isFirstChunk {
                    if let id = extractConversationId(from: chunk) {
                        session.conversationId = id
                        // Skip appending this initial JSON chunk (conversation_id metadata)
                        isFirstChunk = false
                        continue
                    }
                    isFirstChunk = false
                }

                if chunk.hasPrefix("Error: ") {
                    break
                }

                fullContent += chunk
                messages[messageIndex].content = fullContent
                session.messages = messages
            }

            messages[messageIndex].state = .complete
            session.messages = messages

            // Notify that chat has been updated
            NotificationCenter.default.post(name: .chatUpdated, object: nil)
        } catch {
            print("Error: Chat streaming error: \(error.localizedDescription)")
        }
        isLoading = false
    }

    private func extractConversationId(from chunk: String) -> String? {
        if let data = chunk.data(using: .utf8),
           let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           let id = json["conversation_id"] as? String {
            return id
        }
        return nil
    }
}

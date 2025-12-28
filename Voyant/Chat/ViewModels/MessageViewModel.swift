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
    @Published var chatTitle: String

    private var session: ChatSession

    private let backendService = BackendService.shared

    private let userToken: String

    init(session: ChatSession, userToken: String) {
        self.session = session
        self.userToken = userToken
        self.messages = session.messages
        self.chatTitle = session.title
    }

    var conversationId: String? {
        session.conversationId
    }

    func sendMessage() {
        guard !inputMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !isLoading else { return }

        let isFirstMessage = messages.isEmpty

        let trimmedInput = inputMessage.trimmingCharacters(in: .whitespacesAndNewlines)
        let userMessage = ChatMessage(content: trimmedInput, role: .user)
        messages.append(userMessage)
        session.messages = messages
        session.lastActiveDate = Date()

        // Instantly add new chat to the list when first message is sent
        if isFirstMessage {
            NotificationCenter.default.post(
                name: .chatCreated,
                object: nil,
                userInfo: ["session": session]
            )
        }

        let userInput = trimmedInput
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
            var fullContent = ""

            for await chunk in stream {
                // Metadata chunks (conversation_id/title) can arrive at any time; never append them to assistant text.
                if chunk.first == "{", let (id, title) = extractMetadata(from: chunk) {
                    print("[MessageViewModel] Extracted metadata - id: \(id ?? "nil"), title: \(title ?? "nil")")
                    if let id = id {
                        session.conversationId = id
                    }
                    if let title = title {
                        print("[MessageViewModel] Updating title to: '\(title)'")
                        updateTitle(title)
                    }
                    continue
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

    private func updateTitle(_ title: String) {
        print("[MessageViewModel] updateTitle called with: '\(title)'")
        self.chatTitle = title
        self.session.title = title

        // Broadcast the title update
        if let conversationId = session.conversationId {
            print("[MessageViewModel] Broadcasting title update for conversation: \(conversationId)")
            NotificationCenter.default.post(
                name: .chatTitleUpdated,
                object: nil,
                userInfo: [
                    "conversationId": conversationId,
                    "title": title
                ]
            )
        } else {
            print("[MessageViewModel] WARNING: No conversationId yet, cannot broadcast title update")
        }
    }

    private func extractMetadata(from chunk: String) -> (String?, String?)? {
        if let data = chunk.data(using: .utf8),
           let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            let id = json["conversation_id"] as? String
            let title = json["title"] as? String
            if id != nil || title != nil {
                return (id, title)
            }
        }
        return nil
    }
}

//
//  ChatHistoryViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 09.07.2025.
//

import Foundation
import SwiftUI

@MainActor
class ChatHistoryViewModel: ObservableObject {

    @Published var chatSessions: [ChatSession] = []
    @Published var isLoading: Bool = false

    private let userToken: String

    init(userToken: String) {
        self.userToken = userToken

        // Listen for title updates
        NotificationCenter.default.addObserver(
            forName: .chatTitleUpdated,
            object: nil,
            queue: .main
        ) { [weak self] notification in
            self?.handleTitleUpdate(notification)
        }

        // Listen for new chat creation
        NotificationCenter.default.addObserver(
            forName: .chatCreated,
            object: nil,
            queue: .main
        ) { [weak self] notification in
            self?.handleChatCreated(notification)
        }
    }

    private func handleTitleUpdate(_ notification: Notification) {
        guard let userInfo = notification.userInfo,
              let conversationId = userInfo["conversationId"] as? String,
              let title = userInfo["title"] as? String else {
            return
        }

        // Update the title in our local sessions array
        if let index = chatSessions.firstIndex(where: { $0.conversationId == conversationId }) {
            chatSessions[index].title = title
            print("[DEBUG] ChatHistoryViewModel: Updated title for \(conversationId) to '\(title)'")
        }
    }

    private func handleChatCreated(_ notification: Notification) {
        guard let userInfo = notification.userInfo,
              let session = userInfo["session"] as? ChatSession else {
            return
        }

        // Add the new session at the top if it doesn't exist
        if !chatSessions.contains(where: { $0.id == session.id }) {
            chatSessions.insert(session, at: 0)
            print("[DEBUG] ChatHistoryViewModel: Added new chat session instantly")
        }
    }

    func appendSessionIfNeeded(_ session: ChatSession) {
        if !chatSessions.contains(where: { $0.id == session.id }) {
            chatSessions.insert(session, at: 0)
        }
    }

    func updateSessionTitle(conversationId: String, title: String) {
        if let index = chatSessions.firstIndex(where: { $0.conversationId == conversationId }) {
            chatSessions[index].title = title
        }
    }

    func loadChatSessions() {
        if isLoading {
            print("[DEBUG] ChatHistoryViewModel: Already loading, skipping request")
            return
        }
        print("[DEBUG] ChatHistoryViewModel: Starting to load chat sessions at \(Date())")
        isLoading = true

        // Get a valid token
        Task {
            do {
                let token = try await AuthService.getValidToken()
                print("[DEBUG] ChatHistoryViewModel: Got token, length: \(token.count)")

                BackendService.shared.fetchChatSessions(userToken: token) { sessionTuples in
                    print("[DEBUG] ChatHistoryViewModel: Received \(sessionTuples.count) session tuples at \(Date())")

                    if sessionTuples.isEmpty {
                        print("[DEBUG] ChatHistoryViewModel: No sessions found")
                        self.chatSessions = []
                        self.isLoading = false
                        return
                    }

                    let group = DispatchGroup()
                    var tempSessions: [ChatSession] = []

                    for (conversationId, title, lastActiveDate) in sessionTuples {
                        group.enter()
                        print("[DEBUG] ChatHistoryViewModel: Fetching history for conversation \(conversationId), title = \(title ?? "nil"), lastActiveDate = \(lastActiveDate?.description ?? "nil")")
                        BackendService.shared.fetchChatHistory(conversationId: conversationId, userToken: token) { messages in
                            let session = ChatSession(conversationId: conversationId, title: title ?? "New Chat", messages: messages, lastActiveDate: lastActiveDate)
                            tempSessions.append(session)
                            print("[DEBUG] ChatHistoryViewModel: Added session with \(messages.count) messages for \(conversationId), lastActiveDate = \(lastActiveDate?.description ?? "nil")")
                            group.leave()
                        }
                    }

                    group.notify(queue: .main) {
                        // All chat histories have been loaded
                        // Filter out sessions with no messages (empty conversations)
                        let validSessions = tempSessions.filter { !$0.messages.isEmpty }
                        self.chatSessions = validSessions.sorted { $0.lastActiveDate ?? Date.distantPast > $1.lastActiveDate ?? Date.distantPast }
                        print("[DEBUG] ChatHistoryViewModel: Completed loading \(self.chatSessions.count) valid chat sessions (filtered from \(tempSessions.count) total) at \(Date())")
                        self.isLoading = false
                    }
                }
            } catch {
                print("[DEBUG] ChatHistoryViewModel: Failed to get fresh token: \(error)")
                self.isLoading = false
            }
        }
    }

    func loadChatSessionsSilent() {
        // Silent refresh without showing loading state
        print("[DEBUG] ChatHistoryViewModel: Starting silent refresh at \(Date())")

        // Get a valid token
        Task {
            do {
                let token = try await AuthService.getValidToken()
                print("[DEBUG] ChatHistoryViewModel: Got token for silent refresh, length: \(token.count)")

                BackendService.shared.fetchChatSessions(userToken: token) { sessionTuples in
                    print("[DEBUG] ChatHistoryViewModel: Silent refresh received \(sessionTuples.count) session tuples")

                    if sessionTuples.isEmpty {
                        print("[DEBUG] ChatHistoryViewModel: Silent refresh - no sessions found")
                        DispatchQueue.main.async {
                            self.chatSessions = []
                        }
                        return
                    }

                    let group = DispatchGroup()
                    var tempSessions: [ChatSession] = []

                    for (conversationId, title, lastActiveDate) in sessionTuples {
                        group.enter()
                        BackendService.shared.fetchChatHistory(conversationId: conversationId, userToken: token) { messages in
                            let session = ChatSession(conversationId: conversationId, title: title ?? "New Chat", messages: messages, lastActiveDate: lastActiveDate)
                            tempSessions.append(session)
                            group.leave()
                        }
                    }

                    group.notify(queue: .main) {
                        // All chat histories have been loaded silently
                        let validSessions = tempSessions.filter { !$0.messages.isEmpty }
                        self.chatSessions = validSessions.sorted { $0.lastActiveDate ?? Date.distantPast > $1.lastActiveDate ?? Date.distantPast }
                        print("[DEBUG] ChatHistoryViewModel: Silent refresh completed with \(self.chatSessions.count) sessions")
                    }
                }
            } catch {
                print("[DEBUG] ChatHistoryViewModel: Silent refresh failed: \(error)")
            }
        }
    }
}
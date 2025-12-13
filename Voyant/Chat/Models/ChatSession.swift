//
//  ChatSession.swift
//  HealthPredictor
//
//  Created by Stephan  on 09.07.2025.
//

import Foundation

struct ChatSession: Identifiable, Hashable, Codable {
    var conversationId: String?
    var title: String
    var messages: [ChatMessage]
    var lastActiveDate: Date?
    private var localId: String = UUID().uuidString

    init(conversationId: String? = nil, title: String = "New Chat", messages: [ChatMessage] = [], lastActiveDate: Date? = nil) {
        self.conversationId = conversationId
        self.title = title
        self.messages = messages
        self.lastActiveDate = lastActiveDate
    }

    enum CodingKeys: String, CodingKey {
        case conversationId = "conversation_id"
        case title
        case lastActiveDate = "last_active_date"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        self.conversationId = try? container.decodeIfPresent(String.self, forKey: .conversationId)
        self.title = (try? container.decodeIfPresent(String.self, forKey: .title)) ?? "New Chat"
        self.messages = []
        self.lastActiveDate = try? container.decodeIfPresent(Date.self, forKey: .lastActiveDate)
    }

    // - For existing sessions: returns conversation_id from backend
    // - For new local sessions (that do not have at least a single message from the user): returns temporary UUID until backend assigns conversation_id
    var id: String { conversationId ?? localId }

    func hash(into hasher: inout Hasher) {
        hasher.combine(conversationId ?? localId)
    }

    static func == (lhs: ChatSession, rhs: ChatSession) -> Bool {
        if let l = lhs.conversationId, let r = rhs.conversationId {
            return l == r
        }
        return lhs.localId == rhs.localId
    }
}
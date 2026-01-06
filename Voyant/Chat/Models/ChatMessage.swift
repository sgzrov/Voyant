//
//  ChatMessage.swift
//  HealthPredictor
//
//  Created by Stephan  on 18.06.2025.
//

import Foundation

enum MessageSender: String, Codable, Equatable {
    case user
    case assistant
}

enum MessageState: String, Codable, Equatable {
    case complete
    case streaming
    case error
}

struct ChatMessage: Identifiable, Equatable, Codable {
    var id: UUID
    var content: String
    var state: MessageState
    let role: MessageSender

    init(id: UUID = UUID(), content: String, role: MessageSender, state: MessageState = .complete) {
        self.id = id
        self.content = content
        self.state = state
        self.role = role
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        let intId = try container.decode(Int.self, forKey: .id)
        let uuidString = "\(String(format: "%08d", intId))-0000-0000-0000-000000000000"
        guard let uuid = UUID(uuidString: uuidString) else {
            throw DecodingError.dataCorruptedError(forKey: .id, in: container, debugDescription: "Failed to convert integer ID \(intId) to UUID")
        }
        print("[CHAT_MESSAGE_ID] Converted backend ID \(intId) to UUID: \(uuid)")

        self.id = uuid
        self.content = try container.decode(String.self, forKey: .content)
        self.state = (try? container.decode(MessageState.self, forKey: .state)) ?? .complete
        self.role = (try? container.decode(MessageSender.self, forKey: .role)) ?? .assistant
    }

    static func == (lhs: ChatMessage, rhs: ChatMessage) -> Bool {
        lhs.id == rhs.id &&
        lhs.content == rhs.content &&
        lhs.role == rhs.role &&
        lhs.state == rhs.state
    }
}

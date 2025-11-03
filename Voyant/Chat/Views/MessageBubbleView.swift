//
//  MessageBubbleView.swift
//  HealthPredictor
//
//  Created by Stephan  on 18.06.2025.
//

import SwiftUI

struct MessageBubbleView: View {

    let message: ChatMessage

    var body: some View {
        HStack {
            if message.role == .user {
                Spacer()
            }

            VStack(alignment: message.role == .user ? .trailing : .leading, spacing: 4) {
                HStack(alignment: .top, spacing: 8) {
                    Text(message.content.isEmpty && message.state == .streaming ? "Thinking..." : message.content)
                        .padding(.horizontal, 16)
                        .padding(.vertical, 10)
                        .background(message.role == .user ? Color.accentColor : .secondary.opacity(0.3))
                        .foregroundColor(message.role == .user ? .white : .primary)
                        .cornerRadius(20)

                }
            }

            if message.role == .assistant {
                Spacer()
            }
        }
        .transition(.asymmetric(
            insertion: .scale(scale: 0.8).combined(with: .opacity),
            removal: .scale(scale: 0.8).combined(with: .opacity)
        ))
    }
}

#Preview {
    VStack {
        MessageBubbleView(message: ChatMessage(
            content: "Hello! How can I help you with your health today?",
            role: .assistant,
            state: .complete
        ))
        MessageBubbleView(message: ChatMessage(
            content: "Tell me more about my heart rate!",
            role: .user,
            state: .complete
        ))
    }
}


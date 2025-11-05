//
//  ChatView.swift
//  HealthPredictor
//
//  Created by Stephan  on 17.06.2025.
//

import SwiftUI

struct ChatView: View {

    @StateObject private var messageVM: MessageViewModel

    @State private var hasSentFirstMessage = false

    private var session: ChatSession

    private let userToken: String

    var newSessionHandler: ((ChatSession) -> Void)?

    // For previous chats
    init(session: ChatSession, userToken: String) {
        self.session = session
        self.userToken = userToken
        self._messageVM = StateObject(wrappedValue: MessageViewModel(session: session, userToken: userToken))
        self.newSessionHandler = nil
    }

    // For new chats
    init(userToken: String, newSessionHandler: @escaping (ChatSession) -> Void) {
        let newSession = ChatSession()
        self.session = newSession
        self.userToken = userToken
        self._messageVM = StateObject(wrappedValue: MessageViewModel(session: newSession, userToken: userToken))
        self.newSessionHandler = newSessionHandler
    }

    var body: some View {
        VStack(spacing: 0) {
            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(spacing: 12) {
                        ForEach(messageVM.messages) { message in
                            MessageBubbleView(message: message)
                        }
                    }
                    .padding()
                }
                .onChange(of: messageVM.messages) { oldValue, newValue in
                    withAnimation {
                        proxy.scrollTo(newValue.last?.id, anchor: .bottom)
                    }
                    if !hasSentFirstMessage && oldValue.isEmpty && !newValue.isEmpty {
                        // Only append to history once the conversation_id is known
                        if session.conversationId != nil {
                            hasSentFirstMessage = true
                            newSessionHandler?(session)
                        }
                    }
                }
            }

            ChatInputView(
                inputMessage: $messageVM.inputMessage,
                isLoading: messageVM.isLoading,
                onSend: {
                    messageVM.sendMessage()
                }
            )
        }
        .background(Color(.systemGroupedBackground))
        .navigationTitle(session.title)
        .navigationBarTitleDisplayMode(.inline)

    }
}

#Preview {
    ChatView(session: ChatSession(title: "Test Chat"), userToken: "PREVIEW_TOKEN")
}

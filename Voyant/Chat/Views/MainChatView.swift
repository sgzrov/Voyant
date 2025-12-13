//
//  MainChatView.swift
//  HealthPredictor
//
//  Created by Stephan  on 09.07.2025.
//

import SwiftUI

struct MainChatView: View {

    @Environment(\.colorScheme) private var colorScheme

    @StateObject private var chatHistoryVM: ChatHistoryViewModel

    @State private var navigateToChat: ChatSession?

    private let userToken: String

    init(userToken: String) {
        self.userToken = userToken
        _chatHistoryVM = StateObject(wrappedValue: ChatHistoryViewModel(userToken: userToken))
    }

    var borderColor: Color? {
        colorScheme == .dark ? Color.gray.opacity(0.4) : nil
    }

    private func formattedDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "MMM d yyyy 'at' HH:mm"
        let calendar = Calendar.current
        let day = calendar.component(.day, from: date)
        let suffix: String
        switch day {
        case 1, 21, 31: suffix = "st"
        case 2, 22: suffix = "nd"
        case 3, 23: suffix = "rd"
        default: suffix = "th"
        }
        let base = formatter.string(from: date)
        let dayString = String(day)
        if let range = base.range(of: dayString) {
            return base.replacingCharacters(in: range, with: dayString + suffix)
        }
        return base
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Color(.systemGroupedBackground).ignoresSafeArea()
                ScrollView {
                    if chatHistoryVM.isLoading {
                        VStack {
                            Spacer(minLength: 120)
                            ProgressView("Loading chats...")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                            Spacer()
                        }
                    } else if chatHistoryVM.chatSessions.isEmpty {
                        VStack {
                            Spacer(minLength: 120)
                            Text("Tap + to start a new conversation.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                            Spacer()
                        }
                    } else {
                        LazyVStack(spacing: 16) {
                            ForEach(chatHistoryVM.chatSessions) { session in
                                Button(action: {
                                    navigateToChat = session
                                }) {
                                    ZStack(alignment: .leading) {
                                        RoundedRectangle(cornerRadius: 12)
                                            .fill(Color(.systemBackground))
                                            .shadow(color: borderColor == nil ? Color.clear : Color.black.opacity(0.4), radius: 4, x: 0, y: 2)
                                        VStack(alignment: .leading, spacing: 4) {
                                            HStack {
                                                Text(session.title)
                                                    .font(.headline)
                                                    .foregroundColor(.primary)
                                                Spacer()
                                                if let lastActiveDate = session.lastActiveDate {
                                                    Text(formattedDate(lastActiveDate))
                                                        .font(.caption)
                                                        .foregroundColor(.secondary)
                                                } else {
                                                    Text("No date")
                                                        .font(.caption)
                                                        .foregroundColor(.secondary)
                                                }
                                            }
                                            if let lastMessage = session.messages.last {
                                                Text(lastMessage.content)
                                                    .font(.subheadline)
                                                    .foregroundColor(.secondary)
                                                    .lineLimit(1)
                                            } else {
                                                Text("No messages yet")
                                                    .padding(.top, 2)
                                                    .font(.subheadline)
                                                    .foregroundColor(.secondary)
                                                    .italic()
                                            }
                                        }
                                        .padding(16)
                                    }
                                    .overlay(
                                        Group {
                                            if let borderColor = borderColor {
                                                RoundedRectangle(cornerRadius: 12)
                                                    .stroke(borderColor, lineWidth: 1)
                                            }
                                        }
                                    )
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .padding(.vertical, 4)
                                }
                                .buttonStyle(PlainButtonStyle())
                            }
                        }
                        .padding(.top, 12)
                        .padding(.horizontal, 16)
                    }
                }
                .refreshable {
                    _ = try? await AuthService.getValidToken()
                    chatHistoryVM.loadChatSessions()
                }
                .background(Color.clear)
                .navigationTitle("Chats")
                .onAppear {
                    if chatHistoryVM.chatSessions.isEmpty && !chatHistoryVM.isLoading {  // Refresh the chats only once (when the MainChatView first appears)
                        Task {
                            _ = try? await AuthService.getValidToken()
                        }
                        chatHistoryVM.loadChatSessions()
                    }
                }
                // Silent refresh when a chat is updated (no loading indicator)
                .onReceive(NotificationCenter.default.publisher(for: .chatUpdated)) { _ in
                    chatHistoryVM.loadChatSessionsSilent()
                }
                .toolbar {
                    ToolbarItem(placement: .navigationBarTrailing) {
                        Button(action: {
                            let newSession = ChatSession()
                            navigateToChat = newSession
                        }) {
                            ZStack {
                                Circle()
                                    .fill(Color(.secondarySystemFill))
                                    .frame(width: 30, height: 30)
                                Image(systemName: "plus")
                                    .resizable()
                                    .frame(width: 14, height: 14)
                                    .foregroundColor(Color(.systemGroupedBackground))
                            }
                        }
                    }
                }
                .navigationDestination(item: $navigateToChat) { session in
                    ChatView(session: session, userToken: userToken)
                }
            }
        }
    }
}

extension Notification.Name {
    static let chatUpdated = Notification.Name("chatUpdated")
    static let chatTitleUpdated = Notification.Name("chatTitleUpdated")
    static let chatCreated = Notification.Name("chatCreated")
}

#Preview {
    MainChatView(userToken: "PREVIEW_TOKEN")
}

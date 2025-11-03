//
//  SettingsView.swift
//  HealthPredictor
//
//  Created by Stephan  on 13.07.2025.
//

import SwiftUI
import Clerk

struct SettingsView: View {

    @Environment(Clerk.self) private var clerk
    var onSignOut: (() -> Void)?

    var body: some View {
        NavigationStack {
            Form {
                Section(header: Text("Profile")) {
                    if let user = clerk.user {
                        HStack {
                            Image(systemName: "person.crop.circle")
                                .font(.largeTitle)
                                .foregroundColor(.accentColor)
                            VStack(alignment: .leading) {
                                Text(user.id)
                                    .font(.headline)
                                if let email = user.emailAddresses.first?.emailAddress {
                                    Text(email)
                                        .font(.subheadline)
                                        .foregroundColor(.secondary)
                                }
                            }
                        }
                    } else {
                        Text("Not signed in")
                            .foregroundColor(.secondary)
                    }
                }

                Section(header: Text("App Info")) {
                    HStack {
                        Text("Version")
                        Spacer()
                        Text(Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0")
                            .foregroundColor(.secondary)
                    }
                }

                Section {
                    Button(role: .destructive) {
                        Task {
                            try? await clerk.signOut()
                            onSignOut?()
                        }
                    } label: {
                        HStack {
                            Image(systemName: "arrow.backward.circle")
                            Text("Sign Out")
                        }
                    }
                }
            }
            .navigationTitle("Settings")
        }
    }
}

#Preview {
    SettingsView()
        .environment(Clerk.shared)
}

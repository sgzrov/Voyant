//
//  ContentView.swift
//  HealthPredictor
//
//  Created by Stephan  on 13.07.2025.
//

import SwiftUI
import Clerk

struct ContentView: View {

    @Environment(Clerk.self) private var clerk

    var body: some View {
        Group {
            if clerk.user != nil {
                MainTabView()
                    .onAppear {
                        print("User authenticated, showing MainTabView")  // Debug print
                    }
            } else {
                SignInView()
                    .onAppear {
                        print("No user, showing SignInView")  // Debug print
                    }
            }
        }
        .onAppear {
            print("ContentView appeared, clerk.user: \(clerk.user?.id ?? "nil"), clerk.isLoaded: \(clerk.isLoaded)")  // Debug print
        }
    }
}

#Preview {
    ContentView()
        .environment(Clerk.shared)
}

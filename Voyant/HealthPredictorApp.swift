//
//  HealthPredictorApp.swift
//  HealthPredictor
//
//  Created by Stephan  on 23.03.2025.
//

import SwiftUI
import Clerk

private func getClerkPublishableKey() -> String? {
    guard
        let url = Bundle.main.url(forResource: "Secrets", withExtension: "plist"),
        let data = try? Data(contentsOf: url),
        let plist = try? PropertyListSerialization.propertyList(from: data, options: [], format: nil) as? [String: Any],
        let key = plist["CLERK_PUBLISHABLE_KEY"] as? String
    else {
        return nil
    }
    return key
}

@main
struct HealthPredictorApp: App {

    @State private var clerk = Clerk.shared

    init() {
        HealthStoreService.shared.requestAuthorization { success, error in
            print(success ? "HealthKit authorized." : "HealthKit failed: \(error?.localizedDescription ?? "Unknown error")")
        }
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environment(clerk)
                .task {
                    if let key = getClerkPublishableKey() {
                        clerk.configure(publishableKey: key)
                        try? await clerk.load()

                    } else {
                        print("Missing Clerk publishable key")
                    }
                }
        }
    }
}

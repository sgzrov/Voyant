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
        // Track timezone changes so historical events can be displayed in the timezone they occurred in.
        TimezoneHistoryService.shared.start()
        // Track a sparse geo-derived timezone history (more reliable than HK workout metadata).
        GeoTimezoneHistoryService.shared.start()

        // Request location authorization first (to seed timezone inference), but DO NOT block HealthKit on the
        // user's response. iOS will naturally queue permission prompts; HealthKit should be requested either way.
        DispatchQueue.main.async {
            HealthStoreService.shared.requestAuthorization { success, error in
                if success {
                    print("HealthKit authorized.")
                    // Enable background observers immediately after authorization
                    // This allows health data to sync automatically without requiring app launch
                    HealthSyncService.shared.enableBackgroundObservers()
                } else {
                    print("HealthKit failed: \(error?.localizedDescription ?? "Unknown error")")
                }
            }
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

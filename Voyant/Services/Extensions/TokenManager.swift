//
//  TokenManager.swift
//  HealthPredictor
//
//  Created by Stephan on 28.07.2025.
//

import Foundation

class TokenManager {
    static let shared = TokenManager()

    private var cachedToken: String?
    private var tokenExpiry: Date?
    private let refreshBuffer: TimeInterval = 5 // 5-second buffer before expiry
    private let clerkTokenExpiry: TimeInterval = 15 // Clerk tokens expire very quickly

    private init() {}

        func getValidToken() async throws -> String {   // Check if cached token is still valid (include buffer)
        if let token = cachedToken,
           let expiry = tokenExpiry,
           expiry > Date().addingTimeInterval(refreshBuffer) {
            print("[DEBUG] TokenManager: Using cached token (expires in \(Int(expiry.timeIntervalSinceNow)) seconds)")
            return token
        }

        // Get fresh token when needed
        let newToken = try await AuthService.getAuthToken()
        cachedToken = newToken
        tokenExpiry = Date().addingTimeInterval(clerkTokenExpiry)

        print("[DEBUG] TokenManager: Cached fresh token (expires at \(tokenExpiry?.description ?? "unknown"))")
        return newToken
    }

    func forceRefreshToken() async throws -> String {
        print("[DEBUG] TokenManager: Force refreshing token due to presumed 401 error")
        clearCachedToken()
        return try await getValidToken()
    }

    func clearCachedToken() {
        cachedToken = nil
        tokenExpiry = nil
    }
}
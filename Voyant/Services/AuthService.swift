//
//  AuthService.swift
//  HealthPredictor
//
//  Created by Stephan on 13.07.2025.
//

import Foundation
import Clerk

class AuthService {

    static let shared = AuthService()
    static let backendBaseURL: String = {
        if let url = Bundle.main.object(forInfoDictionaryKey: "BACKEND_BASE_URL") as? String, !url.isEmpty {
            return url
        }
        fatalError("BACKEND_BASE_URL not configured in Info.plist")
    }()

    private static var cachedToken: String?
    private static var tokenExpiry: Date?
    private static let refreshBuffer: TimeInterval = 5
    private static let clerkTokenExpiry: TimeInterval = 15

    private init() {}

    // Create an authenticated URLRequest with the Clerk JWT token
    func authenticatedRequest(for endpoint: String, method: String = "GET", body: Data? = nil) async throws -> URLRequest {
        let baseURL = Self.backendBaseURL
        guard let url = URL(string: "\(baseURL)\(endpoint)") else {
            print("🔍 AUTH: Invalid URL: \(baseURL)\(endpoint)")
            throw AuthError.invalidURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = method

        let token = try await Self.getValidToken()
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        // User timezone for backend date-boundary localization
        request.setValue(TimeZone.current.identifier, forHTTPHeaderField: "X-User-TZ")

        if let body = body {
            if request.value(forHTTPHeaderField: "Content-Type") == nil {
                request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            } else {
                print("🔍 AUTH: Content-Type already set: \(request.value(forHTTPHeaderField: "Content-Type") ?? "nil")")
            }
            request.httpBody = body
        }

        return request
    }

    // Get the current user's JWT token from Clerk
    static func getAuthToken() async throws -> String {
        guard let session = await Clerk.shared.session else {
            print("AUTH: No session found in Clerk.shared.session")
            throw AuthError.notAuthenticated
        }
        do {
            let tokenResource = try await session.getToken()
            guard let jwt = tokenResource?.jwt else {
                print("🔍 AUTH: No JWT found in TokenResource")
                throw AuthError.notAuthenticated
            }
            return jwt
        } catch {
            print("🔍 AUTH: Error getting JWT token: \(error)")
            throw AuthError.notAuthenticated
        }
    }

    static func getValidToken() async throws -> String {
        if let token = cachedToken,
           let expiry = tokenExpiry,
           expiry > Date().addingTimeInterval(refreshBuffer) {
            print("[DEBUG] AuthService: Using cached token (expires in \(Int(expiry.timeIntervalSinceNow)) seconds)")
            return token
        }
        let newToken = try await getAuthToken()
        cachedToken = newToken
        tokenExpiry = Date().addingTimeInterval(clerkTokenExpiry)
        print("[DEBUG] AuthService: Cached fresh token (expires at \(tokenExpiry?.description ?? "unknown"))")
        return newToken
    }

    // Decode the JWT and return the Clerk user id (sub)
    static func getUserId() async throws -> String {
        // First try to get from persisted storage (for background launches)
        if let persistedUserId = UserDefaults.standard.string(forKey: "clerk_user_id"),
           !persistedUserId.isEmpty {
            print("[DEBUG] AuthService: Using persisted user ID for background sync")
            return persistedUserId
        }

        // Otherwise get from JWT and persist it
        let jwt = try await getValidToken()
        let parts = jwt.split(separator: ".")
        guard parts.count >= 2 else { throw AuthError.notAuthenticated }
        let payloadB64 = String(parts[1])
        var payloadPadded = payloadB64.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        while payloadPadded.count % 4 != 0 { payloadPadded.append("=") }
        guard let data = Data(base64Encoded: payloadPadded),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let sub = json["sub"] as? String
        else {
            throw AuthError.notAuthenticated
        }

        // Persist for future background launches
        UserDefaults.standard.set(sub, forKey: "clerk_user_id")
        print("[DEBUG] AuthService: Persisted user ID for background sync")

        return sub
    }

    static func forceRefreshToken() async throws -> String {
        print("[DEBUG] AuthService: Force refreshing token due to presumed 401 error")
        clearCachedToken()
        return try await getValidToken()
    }

    static func clearCachedToken() {
        cachedToken = nil
        tokenExpiry = nil
    }

    // Call this on sign out to clear persisted user data
    static func clearPersistedUserData() {
        UserDefaults.standard.removeObject(forKey: "clerk_user_id")
        print("[DEBUG] AuthService: Cleared persisted user data")
    }
}

enum AuthError: Error {
    case notAuthenticated
    case invalidURL
}
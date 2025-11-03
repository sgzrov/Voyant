//
//  AuthService.swift
//  HealthPredictor
//
//  Created by Stephan on 13.07.2025.
//

import Foundation
import Clerk

class AuthService: AuthServiceProtocol {

    static let shared = AuthService()

    private init() {}

    private let baseURL = APIConstants.baseURL

    // Create an authenticated URLRequest with the Clerk JWT token
    func authenticatedRequest(for endpoint: String, method: String = "GET", body: Data? = nil) async throws -> URLRequest {
        guard let url = URL(string: "\(baseURL)\(endpoint)") else {
            print("🔍 AUTH: Invalid URL: \(baseURL)\(endpoint)")
            throw AuthError.invalidURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = method

        let token = try await Self.getAuthToken()
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

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
}

enum AuthError: Error {
    case notAuthenticated
    case invalidURL
}
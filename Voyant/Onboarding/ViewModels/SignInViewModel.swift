//
//  SignInViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 13.07.2025.
//

import Foundation
import Clerk
import AuthenticationServices

@MainActor
class SignInViewModel: ObservableObject {

    @Published var isLoading = false
    @Published var errorMessage = ""

    init() {}

    func handleAppleSignIn(_ result: Result<ASAuthorization, Error>) async {
        isLoading = true
        errorMessage = ""

        do {
            guard let credential = try result.get().credential as? ASAuthorizationAppleIDCredential else {
                errorMessage = "Unable to get Apple ID credential"
                return
            }
            guard let idToken = credential.identityToken.flatMap({ String(data: $0, encoding: .utf8) }) else {
                errorMessage = "Unable to get ID token from Apple"
                return
            }
            try await SignIn.authenticateWithIdToken(provider: .apple, idToken: idToken)
        } catch {
            errorMessage = "Apple sign in failed: \(error.localizedDescription)"
        }

        isLoading = false
    }

    func handleGoogleSignIn() async {
        isLoading = true
        errorMessage = ""

        do {
            try await SignIn.authenticateWithRedirect(strategy: .oauth(provider: .google))
        } catch {
            errorMessage = "Google sign in failed: \(error.localizedDescription)"
        }

        isLoading = false
    }
}

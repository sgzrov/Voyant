//
//  AuthView.swift
//  HealthPredictor
//
//  Created by Stephan  on 13.07.2025.
//

import SwiftUI
import Clerk
import AuthenticationServices

struct SignInView: View {

    @StateObject private var signInVM = SignInViewModel()

    @Environment(\.colorScheme) private var colorScheme

    var body: some View {
        VStack(spacing: 20) {
            Spacer()

            VStack(spacing: 16) {
                SignInWithAppleButton { request in
                    request.requestedScopes = [.email, .fullName]
                    request.nonce = UUID().uuidString
                } onCompletion: { result in
                    Task {
                        await signInVM.handleAppleSignIn(result)
                    }
                }
                .frame(height: 50)
                .disabled(signInVM.isLoading)

                Button {
                    Task {
                        await signInVM.handleGoogleSignIn()
                    }
                } label: {
                    HStack {
                        Image("icons8-google-48")
                            .resizable()
                            .frame(width: 20, height: 20)
                        Text("Sign in with Google")
                            .fontWeight(.medium)
                    }
                    .frame(maxWidth: .infinity)
                    .frame(height: 50)
                    .background(colorScheme == .dark ? Color.gray.opacity(0.2) : Color.white)
                    .foregroundColor(colorScheme == .dark ? .white : .black)
                    .cornerRadius(8)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(Color.gray.opacity(colorScheme == .dark ? 0.4 : 0.3), lineWidth: 1)
                    )
                }
                .disabled(signInVM.isLoading)

            }
            .padding(.horizontal, 40)

            Spacer()
        }
        .background(Color.primary.opacity(0.05))
    }
}

#Preview {
    SignInView()
}

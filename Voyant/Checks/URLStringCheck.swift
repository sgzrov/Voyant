//
//  URLStringCheck.swift
//  HealthPredictor
//
//  Created by Stephan  on 28.05.2025.
//

import Foundation

struct URLStringCheck {
    struct ValidationResult {
        let isValid: Bool
        let errorMessage: String?
    }

    private static let invalidURLCharacters = CharacterSet(charactersIn: "<>{}|")
    private static let commonTLDs = ["com", "org", "net", "edu", "gov", "io", "co", "ai", "app", "dev", "health", "research",
    "us", "uk", "de", "fr", "ca", "au", "nz", "se", "no", "fi", "nl", "ch", "it", "es", "dk", "ie", "be", "at", "jp", "kr", "sg",
    "in", "br", "mx", "za", "is", "cz", "pl", "il", "gr", "ru", "ua", "pt", "ar", "tr", "cl", "my", "th", "hk", "ae"]

    static func validateURL(_ urlString: String) -> ValidationResult {
        let trimmed = urlString.trimmingCharacters(in: .whitespacesAndNewlines)

        // Check for https://
        guard trimmed.lowercased().hasPrefix("https://") else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        // Check for periods
        let afterProtocol = trimmed.dropFirst(8)
        let periodCheck = afterProtocol.components(separatedBy: ".")
        guard periodCheck.count >= 2,
              !periodCheck.contains(where: { $0.isEmpty }),
              !afterProtocol.hasSuffix("."),
              !afterProtocol.hasPrefix("."),
              !afterProtocol.contains("..") else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        // Character validation
        guard !trimmed.contains(" "),
              trimmed.rangeOfCharacter(from: invalidURLCharacters) == nil else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        // URL parsing and host validation
        guard let url = URL(string: trimmed),
              let host = url.host,
              !host.isEmpty else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        // Domain structure validation
        let components = host.components(separatedBy: ".")
        guard components.count >= 2 else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        // TLD validation
        let tld = components.last?.lowercased() ?? ""
        guard commonTLDs.contains(tld) else {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        return ValidationResult(isValid: true, errorMessage: nil)
    }

    static func validatePartialURL(_ urlString: String) -> ValidationResult {
        let partialString = urlString.trimmingCharacters(in: .whitespacesAndNewlines)

        if partialString.isEmpty {
            return ValidationResult(isValid: true, errorMessage: nil)
        }

        // Check for partial https:// input
        let httpsPrefix = "https://"
        if partialString.count <= httpsPrefix.count {
            let currentPrefix = partialString.lowercased()
            if httpsPrefix.hasPrefix(currentPrefix) {
                return ValidationResult(isValid: true, errorMessage: nil)
            }
        }

        // Immediate error for non-https URLs
        if !partialString.lowercased().hasPrefix("https://") {
            return ValidationResult(isValid: false, errorMessage: "Invalid URL. Try again.")
        }

        return validateURL(partialString)
    }
}

//
//  CloudflareCheck.swift
//  HealthPredictor
//
//  Created by Stephan  on 01.06.2025.
//

import Foundation

class CloudflareCheck {

    static let shared = CloudflareCheck()

    private let session: URLSession

    private init() {
        // Create configuration
        let configuration = URLSessionConfiguration.default
        configuration.httpCookieAcceptPolicy = .always
        configuration.httpShouldSetCookies = true
        configuration.timeoutIntervalForRequest = 30
        configuration.timeoutIntervalForResource = 300
        configuration.httpMaximumConnectionsPerHost = 1

        // Set up headers and options to mimic a real browser
        configuration.httpAdditionalHeaders = [
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.8,image/avif,image/webp,image/apng,*/*;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        ]

        // Initialize session
        self.session = URLSession(configuration: configuration)
    }

    func makeRequest(to url: URL) async throws -> (Data, URLResponse) {
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.timeoutInterval = 30

        // Small delay to simulate human behavior
        try await Task.sleep(nanoseconds: UInt64(0.5 * 1_000_000_000))

        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw NSError(domain: "CloudflareCheck", code: -2, userInfo: [NSLocalizedDescriptionKey: "Invalid response type."])
        }

        if httpResponse.statusCode == 403 {
            throw NSError(domain: "CloudflareCheck", code: 403, userInfo: [NSLocalizedDescriptionKey: "Access denied - Cloudflare protection detected."])
        }

        return (data, response)
    }

    func isCloudflareProtected(_ response: URLResponse) -> Bool {
        if let httpResponse = response as? HTTPURLResponse {
            return httpResponse.statusCode == 403
        } else {
            return false
        }
    }
}

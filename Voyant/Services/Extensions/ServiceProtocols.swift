//
//  ServiceProtocols.swift
//  HealthPredictor
//
//  Created by Stephan on 22.06.2025.
//

import Foundation
import Combine
import HealthKit

protocol AuthServiceProtocol {
    func authenticatedRequest(for endpoint: String, method: String, body: Data?) async throws -> URLRequest
}

protocol SSEServiceProtocol {
    func streamSSE(request: URLRequest) async throws -> AsyncStream<String>
}

protocol AgentBackendServiceProtocol {
    func chatWithCIStream(csvFilePath: String, userInput: String, conversationId: String?) async throws -> AsyncStream<String>
    func simpleChatStream(userInput: String, conversationId: String?) async throws -> AsyncStream<String>
    func shouldUseCodeInterpreter(userInput: String) async throws -> Bool
}

protocol UserFileCacheServiceProtocol {
    func getCachedHealthFile() async throws -> String
    func setupCSVFile()
    func performCleanup()
}

protocol UserFileCreationServiceProtocol {
    func generateCSV(completion: @escaping (URL?) -> Void)
    static func metricNames() -> [String]
}

protocol HealthStoreServiceProtocol {
    func requestAuthorization(completion: @escaping (Bool, Error?) -> Void)
    var healthStore: HKHealthStore { get }
}

protocol FileUploadToBackendServiceProtocol {
    func uploadHealthDataFile(fileData: Data) async throws -> String
    func buildMultipartRequest(endpoint: String, fileData: Data, additionalFields: [String: String]) async throws -> URLRequest
}

protocol TextExtractionBackendServiceProtocol {
    func extractTextFromFile(fileURL: URL) async throws -> String
    func extractTextFromURL(urlString: String) async throws -> String
}
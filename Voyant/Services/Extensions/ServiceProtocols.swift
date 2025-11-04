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
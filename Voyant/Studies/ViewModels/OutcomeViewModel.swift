//
//  OutcomeViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 08.06.2025.
//

import Foundation
import SwiftUI

@MainActor
class OutcomeViewModel: ObservableObject {

    @Published var isGenerating = false
    @Published var outcomeText: String?
    @Published var errorMessage: String?

    private let backendService = BackendService.shared
    private let healthFileCacheService = UserFileCacheService.shared

    func generateOutcome(userInput: String, studyId: String, onUpdate: @escaping (String) -> Void) async -> String? {
        isGenerating = true
        outcomeText = nil
        errorMessage = nil

        do {
            guard !userInput.isEmpty else {
                isGenerating = false
                return nil
            }

            let csvPath = try await generateCSVAsync()
            var fullOutcome = ""
            let stream = try await backendService.generateOutcome(csvFilePath: csvPath, userInput: userInput, studyId: studyId)

            for await chunk in stream {
                if chunk.hasPrefix("Error: ") {
                    print("Outcome error chunk: \(chunk)")
                    self.errorMessage = chunk
                    isGenerating = false
                    return nil
                }
                fullOutcome += chunk
                self.outcomeText = fullOutcome

                onUpdate(fullOutcome)

                try await Task.sleep(nanoseconds: 4_000_000)
            }

            isGenerating = false
            return fullOutcome
        } catch {
            self.errorMessage = "Failed to generate outcome: \(error.localizedDescription)"
            print("Exception in generateOutcome: \(error)")
            isGenerating = false
            return nil
        }
    }

    private func extractStudyId(from chunk: String) -> String? {
        if let data = chunk.data(using: .utf8),
           let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           let studyId = json["study_id"] as? String {
            return studyId
        }
        return nil
    }

    private func generateCSVAsync() async throws -> String {
        return try await healthFileCacheService.getCachedHealthFile()
    }
}

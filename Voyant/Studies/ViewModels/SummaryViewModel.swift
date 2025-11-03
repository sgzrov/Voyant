//
//  SummaryViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 05.06.2025.
//

import Foundation
import SwiftUI

@MainActor
class SummaryViewModel: ObservableObject {

    @Published var summarizedText: String?
    @Published var extractedText: String?
    @Published var isSummarizing = false
    @Published var errorMessage: String = ""

    private let backendService = BackendService.shared

    func summarizeStudy(text: String, studyId: String, onUpdate: @escaping (String) -> Void) async -> String? {
        isSummarizing = true
        summarizedText = nil
        errorMessage = ""

        do {
            guard !text.isEmpty else {
                print("No text to summarize.")
                isSummarizing = false
                return nil
            }

            self.extractedText = text
            var fullSummary = ""
            let stream = try await backendService.summarizeStudy(userInput: text, studyId: studyId)

            for await chunk in stream {
                if chunk.hasPrefix("Error: ") {
                    self.errorMessage = chunk
                    isSummarizing = false
                    return nil
                }
                fullSummary += chunk
                self.summarizedText = fullSummary

                onUpdate(fullSummary)

                try await Task.sleep(nanoseconds: 4_000_000)  // Add a small delay to make streaming visible
            }

            isSummarizing = false
            return fullSummary
        } catch {
            self.errorMessage = "Failed to summarize: \(error.localizedDescription)"
            print("Failed to summarize: \(error).")
            isSummarizing = false
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
}

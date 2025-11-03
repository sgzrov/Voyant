//
//  StudyDetailedView.swift
//  HealthPredictor
//
//  Created by Stephan  on 05.06.2025.
//

import SwiftUI

struct StudyDetailedView: View {

    let studyId: String
    let extractedText: String?

    @ObservedObject var studiesVM: StudyViewModel

    @StateObject private var summaryVM = SummaryViewModel()
    @StateObject private var outcomeVM = OutcomeViewModel()

    private var study: Study? {
        studiesVM.studies.first { $0.studyId == studyId }
    }

        var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Summary:")
                        .font(.headline)
                        .foregroundColor(.primary)
                        .frame(maxWidth: .infinity, alignment: .leading)

                    if study?.summary.isEmpty ?? true {
                        Text("Thinking...")
                            .foregroundColor(.secondary)
                            .italic()
                            .frame(maxWidth: .infinity, alignment: .leading)
                    } else {
                        Text(study?.summary ?? "")
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("Outcome:")
                        .font(.headline)
                        .foregroundColor(.primary)
                        .frame(maxWidth: .infinity, alignment: .leading)

                    if study?.outcome.isEmpty ?? true {
                        Text("Thinking...")
                            .foregroundColor(.secondary)
                            .italic()
                            .frame(maxWidth: .infinity, alignment: .leading)
                    } else {
                        Text(study?.outcome ?? "")
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
            }
            .padding()
        }
        .onAppear {
            if study?.studyId != nil && extractedText != nil {
                startStreaming()
            }
        }
    }

    private func startStreaming() {
        guard let extractedText = extractedText,
              let studyId = study?.studyId else {
            return
        }

        // Start summary streaming with real-time updates
        Task {
            await summaryVM.summarizeStudy(text: extractedText, studyId: studyId) { summary in
                print("[DEBUG] StudyDetailedView: Summary callback called with length: \(summary.count)")
                studiesVM.updateStudyInRealTime(studyId: studyId, summary: summary)
            }
        }

        // Start outcome streaming with real-time updates
        Task {
            await outcomeVM.generateOutcome(userInput: extractedText, studyId: studyId) { outcome in
                print("[DEBUG] StudyDetailedView: Outcome callback called with length: \(outcome.count)")
                studiesVM.updateStudyInRealTime(studyId: studyId, outcome: outcome)
            }
        }
    }
}

#Preview {
    StudyDetailedView(
        studyId: "sample-study-id",
        extractedText: nil,
        studiesVM: StudyViewModel(userToken: "preview-token")
    )
}

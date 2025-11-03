//
//  StudyViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 16.06.2025.
//

import Foundation
import SwiftUI

@MainActor
class StudyViewModel: ObservableObject {

    @Published var studies: [Study] = []
    @Published var isLoading: Bool = false
    @Published var errorMessage: String = ""

    private let userToken: String
    private let backendService = BackendService.shared

    init(userToken: String) {
        self.userToken = userToken
    }

    func loadStudies() {
        print("[DEBUG] StudyViewModel: loadStudies called")
        if isLoading {
            print("[DEBUG] StudyViewModel: Already loading, skipping request")
            return
        }
        print("[DEBUG] StudyViewModel: Starting to load studies at \(Date())")
        isLoading = true
        errorMessage = ""

        // Get a valid token from TokenManager
        Task {
            do {
                let token = try await TokenManager.shared.getValidToken()
                print("[DEBUG] StudyViewModel: Got token from TokenManager, length: \(token.count)")

                backendService.fetchStudies(userToken: token) { [weak self] studies in
                    DispatchQueue.main.async {
                        let sortedStudies = studies.sorted { first, second in
                            guard let firstDate = first.importDate, let secondDate = second.importDate else {
                                return first.importDate != nil && second.importDate == nil
                            }
                            return firstDate > secondDate
                        }
                        self?.studies = sortedStudies
                        self?.isLoading = false
                    }
                }
            } catch {
                print("[DEBUG] StudyViewModel: Failed to get fresh token: \(error)")
                self.isLoading = false
            }
        }
    }

    func createStudy(title: String, summary: String = "", outcome: String = "", studyId: String? = nil) {
        isLoading = true
        errorMessage = ""

        // Get a valid token from TokenManager
        Task {
            do {
                let token = try await TokenManager.shared.getValidToken()
                print("[DEBUG] StudyViewModel: Got token for createStudy, length: \(token.count)")

                backendService.createStudy(userToken: token, title: title, summary: summary, outcome: outcome, studyId: studyId) { [weak self] study in
                    DispatchQueue.main.async {
                        if let study = study {
                            self?.studies.insert(study, at: 0)
                        } else {
                            self?.errorMessage = "Failed to create study"
                        }
                        self?.isLoading = false
                    }
                }
            } catch {
                print("[DEBUG] StudyViewModel: Failed to get fresh token for createStudy: \(error)")
                self.isLoading = false
                self.errorMessage = "Failed to get valid token"
            }
        }
    }

    func updateStudyInRealTime(studyId: String, summary: String? = nil, outcome: String? = nil) {
        if let index = studies.firstIndex(where: { $0.studyId == studyId }) {
            var updatedStudy = studies[index]

            if let summary = summary {
                updatedStudy.summary = summary
                print("[DEBUG] StudyViewModel: Updated summary for study \(studyId)")
            }
            if let outcome = outcome {
                updatedStudy.outcome = outcome
                print("[DEBUG] StudyViewModel: Updated outcome for study \(studyId)")
            }

            studies[index] = updatedStudy
            print("[DEBUG] StudyViewModel: Updated study in array at index \(index)")

            objectWillChange.send()
        } else {
            print("[DEBUG] StudyViewModel: Study not found in studies array for studyId: \(studyId)")
        }
    }
}

//
//  StudiesHomeView.swift
//  HealthPredictor
//
//  Created by Stephan  on 24.05.2025.
//

import SwiftUI

struct StudiesHomeView: View {

    @StateObject private var importVM = TagExtractionViewModel()
    @StateObject private var studiesVM: StudyViewModel

    @State private var showSheet: Bool = false
    @State private var showFileImporter: Bool = false
    @State private var selectedFileURL: URL?
    @State private var currentStudy: Study?
    @State private var navigateToStudy: Study?
    @State private var extractedTextForStudy: String?
    @State private var streamingStudy: Study?
    @State private var hasInitiallyLoadedStudies: Bool = false

    init(userToken: String) {
        _studiesVM = StateObject(wrappedValue: StudyViewModel(userToken: userToken))
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Color(.systemGroupedBackground).ignoresSafeArea()

                ScrollView {
                    if studiesVM.isLoading {
                        VStack {
                            Spacer(minLength: 120)
                            ProgressView("Loading studies...")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                            Spacer()
                        }
                    } else if studiesVM.studies.isEmpty {
                        VStack {
                            Spacer(minLength: 120)
                            Text("Tap + to import your first study.")
                                .font(.subheadline)
                                .foregroundColor(.secondary)
                                .multilineTextAlignment(.center)
                            Spacer()
                        }
                    } else {
                        StudiesListView(studiesVM: studiesVM, studies: studiesVM.studies)
                    }
                }
            }
            .navigationDestination(item: $navigateToStudy) { study in
                StudyDetailedView(studyId: study.studyId ?? "", extractedText: extractedTextForStudy, studiesVM: studiesVM)
            }
            .sheet(isPresented: $showSheet) {
                ImportSheetView(
                    importVM: importVM,
                    studiesVM: studiesVM,
                    showFileImporter: $showFileImporter,
                    selectedFileURL: $selectedFileURL,
                    onDismiss: {
                        showSheet = false
                        selectedFileURL = nil
                        importVM.clearInput()
                    },
                    onImport: { study, extractedText in
                        currentStudy = study
                        extractedTextForStudy = extractedText
                        streamingStudy = study
                        navigateToStudy = study
                        showSheet = false
                        selectedFileURL = nil
                        importVM.clearInput()
                    }
                )
            }
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button(action: {
                        showSheet = true
                    }) {
                        ZStack {
                            Circle()
                                .fill(Color(.secondarySystemFill))
                                .frame(width: 30, height: 30)
                            Image(systemName: "plus")
                                .resizable()
                                .frame(width: 14, height: 14)
                                .foregroundColor(Color(.systemGroupedBackground))
                        }
                    }
                }
            }
            .navigationTitle("Studies")
            .onAppear {
                if !hasInitiallyLoadedStudies && !studiesVM.isLoading {
                    Task {
                        _ = try? await TokenManager.shared.getValidToken()
                    }
                    studiesVM.loadStudies()
                    hasInitiallyLoadedStudies = true
                }
            }
            .refreshable {
                _ = try? await TokenManager.shared.getValidToken()
                studiesVM.loadStudies()
            }

        }
    }
}

#Preview {
    StudiesHomeView(userToken: "preview-token")
}

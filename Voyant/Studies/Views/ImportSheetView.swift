//
//  ImportSheetView.swift
//  HealthPredictor
//
//  Created by Stephan  on 25.05.2025.
//

import SwiftUI

struct ImportSheetView: View {

    @ObservedObject var importVM: TagExtractionViewModel
    @ObservedObject var studiesVM: StudyViewModel

    @Binding var showFileImporter: Bool
    @Binding var selectedFileURL: URL?

    @FocusState private var isTextFieldFocused: Bool

    let onDismiss: () -> Void
    let onImport: (Study, String) -> Void

    var body: some View {
        ZStack {
            Color(.systemGroupedBackground).ignoresSafeArea()
            VStack(spacing: 0) {
                ImportSheetHeaderView(showFileImporter: $showFileImporter, onDismiss: onDismiss)
                VStack(spacing: 24) {
                    VStack {
                        Text("Import")
                            .font(.title)
                            .fontWeight(.bold)
                        Text("Health Studies")
                            .font(.title)
                            .fontWeight(.bold)
                    }
                    .padding(.top, 70)
                    Text("Import health studies to view how their findings correlate with your health data.")
                        .font(.headline)
                        .multilineTextAlignment(.center)
                        .foregroundColor(.secondary)
                        .padding(.horizontal, 50)
                }
                ImportSheetInputSection(importVM: importVM, selectedFileURL: $selectedFileURL, isTextFieldFocused: $isTextFieldFocused)
                ImportSheetTagsAndImportButton(importVM: importVM, selectedFileURL: $selectedFileURL, isTextFieldFocused: $isTextFieldFocused, onImport: {

                    // Capture the URL and input before dismissing the view
                    let capturedURL = selectedFileURL
                    let capturedInput = importVM.importInput

                    let url = selectedFileURL ?? URL(string: importVM.importInput)!
                    var study = Study(
                        studyId: nil,
                        title: url.lastPathComponent,
                        summary: "",
                        outcome: "",
                        importDate: Date()
                    )
                    print("[DEBUG] ImportSheetView: Created study with nil studyId, title: \(study.title)")

                    Task {
                        let extractedText: String?

                        if let url = capturedURL, url.isFileURL {
                            extractedText = try? await BackendService.shared.extractTextFromFile(fileURL: url)
                        } else if let url = capturedURL, let scheme = url.scheme, scheme.hasPrefix("http") {
                            extractedText = try? await BackendService.shared.extractTextFromURL(urlString: url.absoluteString)
                        } else if !capturedInput.isEmpty, let url = URL(string: capturedInput), let scheme = url.scheme, scheme.hasPrefix("http") {
                            extractedText = try? await BackendService.shared.extractTextFromURL(urlString: url.absoluteString)
                        } else {
                            extractedText = nil
                        }

                        guard let extractedText, !extractedText.isEmpty else {
                            print("[DEBUG] ImportSheetView: Text extraction failed")
                            return
                        }

                        do {
                            let studyId = try await BackendService.shared.createStudy()  // Feed one study to both the summary/outcome to not get error of mixed persistence (summaries and outcomes are written to different studies)
                            study.studyId = studyId
                            print("[DEBUG] ImportSheetView: Created study with study_id: \(studyId)")

                            // Add the study to the studies array so it can be updated during streaming
                            await MainActor.run {
                                studiesVM.studies.insert(study, at: 0)
                                print("[DEBUG] ImportSheetView: Added study to studies array, total studies: \(studiesVM.studies.count)")
                            }

                            // Pass the study with extracted text to StudyDetailedView
                            await MainActor.run {
                                onImport(study, extractedText)
                                onDismiss()
                            }
                        } catch {
                            print("[DEBUG] ImportSheetView: Failed to create study: \(error)")
                        }
                    }
                })
                Spacer()
            }
            .padding(.horizontal, 16)
        }
        .ignoresSafeArea(.keyboard)
        .onTapGesture {
            isTextFieldFocused = false
        }
        .presentationDetents([.large])
        .animation(.easeInOut(duration: 0.2), value: selectedFileURL)
        .fileImporter(isPresented: $showFileImporter, allowedContentTypes: [.pdf, .plainText, .rtf], allowsMultipleSelection: false) { result in
            importVM.clearTags()

            switch result {
            case .success(let urls):
                if let fileURL = urls.first {
                    selectedFileURL = fileURL
                    Task {
                        await importVM.validateFileType(url: fileURL)
                    }
                }
            case .failure:
                selectedFileURL = nil
            }
        }
    }
}

#Preview {
    ImportSheetView(
        importVM: TagExtractionViewModel(),
        studiesVM: StudyViewModel(userToken: "preview-token"),
        showFileImporter: .constant(false),
        selectedFileURL: .constant(nil),
        onDismiss: {},
        onImport: { _, _ in }
    )
}

//
//  ImportURLViewModel.swift
//  HealthPredictor
//
//  Created by Stephan  on 28.05.2025.
//

import Foundation
import PDFKit

@MainActor
class ImportURLViewModel: ObservableObject {

    @Published var importInput: String = ""
    @Published var errorMessage: String = ""
    @Published var isPDF: Bool = false
    @Published var isHTML: Bool = false
    @Published var isLoading: Bool = false

    var currentSecurityScopedURL: URL?

    func validateURL() {
        let result = URLStringCheck.validatePartialURL(importInput)
        errorMessage = result.errorMessage ?? ""
    }

    func validateFileType(url: URL) async {
        isLoading = true
        resetValidationState()

        if url.isFileURL {
            await validateLocalFile(url: url)
        }

        isLoading = false
    }

    func isFullyValidURL() -> Bool {
        return URLStringCheck.validateURL(importInput).isValid
    }

    func clearInput() {
        importInput = ""
        errorMessage = ""
        isPDF = false
        isHTML = false
    }

    func stopAccessingCurrentFile() {
        if let url = currentSecurityScopedURL {
            url.stopAccessingSecurityScopedResource()
            currentSecurityScopedURL = nil
        }
    }

    private func resetValidationState() {
        errorMessage = ""
        isPDF = false
        isHTML = false
    }

    private func validateLocalFile(url: URL) async {
        stopAccessingCurrentFile()

        let isSecurityScoped = url.startAccessingSecurityScopedResource()  // Handle security-scoped local file URLs
        if isSecurityScoped {
            currentSecurityScopedURL = url
        }

        let fileManager = FileManager.default
        let filePath = url.path
        do {
            _ = try fileManager.attributesOfItem(atPath: filePath)
        } catch {
            errorMessage = "Cannot access the selected file: \(error.localizedDescription)"
            isLoading = false
            return
        }

        let fileExtension = url.pathExtension.lowercased()
        switch fileExtension {
        case "pdf":
            do {
                let data = try await FileUtilities.readFileData(from: url)
                if PDFDocument(data: data) != nil {
                    isPDF = true
                } else {
                    errorMessage = "The file appears to be corrupted or not a valid PDF."
                }
            } catch {
                errorMessage = "Could not read the PDF file: \(error.localizedDescription)"
            }
        case "rtf":
            isHTML = false
        case "txt", "":
            isHTML = false
        default:
            errorMessage = "Unsupported file type. Please select a PDF, RTF, or text file."
        }
    }
}

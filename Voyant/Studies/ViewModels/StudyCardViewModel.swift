import Foundation
import SwiftUI

class StudyCardViewModel: ObservableObject {

    @Published var study: Study

    init(study: Study) {
        self.study = study
    }

    var formattedDate: String {
        guard let date = study.importDate else {
            return "No date"
        }
        return DateUtilities.formatDisplayDate(date)
    }

    var summaryText: String {
        if study.summary.isEmpty {
            return "No summary yet"
        } else {
            // Show first sentence or first 100 characters for preview
            let sentences = study.summary.components(separatedBy: [".", "!", "?"])
            if let firstSentence = sentences.first?.trimmingCharacters(in: .whitespacesAndNewlines), !firstSentence.isEmpty {
                return firstSentence + "."
            } else {
                // Fallback to first 100 characters if no sentence found
                let preview = String(study.summary.prefix(100))
                return preview + (study.summary.count > 100 ? "..." : "")
            }
        }
    }

    var isSummaryEmpty: Bool {
        study.summary.isEmpty
    }

    var title: String {
        study.title
    }
}

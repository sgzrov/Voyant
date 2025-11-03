//
//  StudiesListView.swift
//  HealthPredictor
//
//  Created by Stephan on 24.06.2025.
//

import SwiftUI

struct StudiesListView: View {

    @ObservedObject var studiesVM: StudyViewModel

    let studies: [Study]

    var body: some View {
        LazyVStack(spacing: 16) {
            if studies.isEmpty {
                VStack {
                    Spacer(minLength: 120)
                    Text("Tap + to import a new study.")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                    Spacer()
                }
            }
            ForEach(studies) { study in
                NavigationLink(destination: StudyDetailedView(studyId: study.studyId ?? "", extractedText: nil, studiesVM: studiesVM)) {
                    StudyCardView(viewModel: StudyCardViewModel(study: study))
                }
            }
        }
        .padding(.top, 12)
        .padding(.horizontal, 16)
    }
}

struct StudiesListView_Previews: PreviewProvider {
    static var previews: some View {
        let sampleStudies = [
            Study(
                id: UUID(),
                studyId: "sample-study-1",
                title: "How can high heart rates increase the risk of cancer?",
                summary: "This is a sample summary.",
                outcome: "This is a sample outcome.",
                importDate: Date()
            ),
            Study(
                id: UUID(),
                studyId: "sample-study-2",
                title: "How can a calorie deficit affect brain fog?",
                summary: "This is a sample summary.",
                outcome: "this is a sample outcome.",
                importDate: Date()
            )
        ]
        StudiesListView(studiesVM: StudyViewModel(userToken: "preview-token"), studies: sampleStudies)
            .previewLayout(.sizeThatFits)
    }
}

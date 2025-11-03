//
//  StudyCardView.swift
//  HealthPredictor
//
//  Created by Stephan  on 16.06.2025.
//

import SwiftUI

struct StudyCardView: View {

    @Environment(\.colorScheme) private var colorScheme

    @ObservedObject var viewModel: StudyCardViewModel

    var borderColor: Color? {
        colorScheme == .dark ? Color.gray.opacity(0.5) : nil
    }

    var body: some View {
        ZStack(alignment: .leading) {
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(.systemBackground))
            VStack(alignment: .leading, spacing: 8) {
                HStack(alignment: .top) {
                    Text(viewModel.title)
                        .font(.headline)
                        .foregroundColor(.primary)
                    Spacer()
                    Text(viewModel.formattedDate)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                Text(viewModel.summaryText)
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                    .italic(viewModel.isSummaryEmpty)
                    .lineLimit(1)
            }
            .padding(16)
        }
        .overlay(
            Group {
                if let borderColor = borderColor {
                    RoundedRectangle(cornerRadius: 12)
                        .stroke(borderColor, lineWidth: 1)
                }
            }
        )
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.vertical, 4)
    }
}

#Preview {
    let study = Study(
        studyId: "sample-study-id",
        title: "Sample Study Title",
        summary: "This is a sample study summary.",
        outcome: "This is a sample outcome.",
        importDate: Date()
    )

    let viewModel = StudyCardViewModel(study: study)
    StudyCardView(viewModel: viewModel)
}
